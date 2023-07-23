use anyhow::{anyhow, Ok, Result};
use crc::{Algorithm, Crc};
use memmap::Mmap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};

use crate::utils::utils::{os_str_to_string, is_empty_file};
use crate::utils::varint::{decode_varint, decode_varint_from_mmap, encode_varint_to_vec};

use super::{Bytes, Record, RecordIndex, FLAG_PADDING};

/*
 * Segment Strurt:
 * Max Block Size: 32KB
 * Segment Format:
 * |record1, record2, padding | record1, record2, padding|
 *  <--------block----------->
 *
 * Short Record Format:
 * | Flag(1B) | Key Length(varint) | Value Length(varint) | Key | Value | CRC(4B) |
 *  <-------------------------header---------------------------->
 *
 * Multi Block Record Format:
 * |     Header     |                  Payload                | CRC(4B) | Padding |
 * <-------------block1--------------><----block2----><-----------block3---------->
 *
*/
pub(crate) struct Segment {
    mutable: bool,
    path: PathBuf,
    internal: Mutex<SegmentInternal>, // fd will be changed anyway, no need for RwLock
    mmap: Option<RwLock<Mmap>>,
}

struct SegmentInternal {
    fd: Option<File>,
    block_written: u64,
    segment_written: u64,
    buffer: Vec<u8>,
}

const BLOCK_BYTES: u64 = 32 * 1024; // 32KB
const MAX_SEGMENT_BYTES: u64 = 1024 * 1024 * 1024; // 1GB, , large record may cause segment exceed limit
const CRC_CONFIG: Algorithm<u32> = Algorithm {
    width: 16,
    poly: 0x8005,
    init: 0xffff,
    refin: false,
    refout: false,
    xorout: 0x0000,
    check: 0xaee7,
    residue: 0x0000,
};

pub(crate) struct WriteResult {
    pub(crate) is_segment_full: bool,
    pub(crate) begin_offset: u64,
}

impl Segment {
    // create a segment, but do not open fd
    pub(crate) fn open_read_only(path: PathBuf) -> Self {
        Self {
            mutable: false,
            path,
            mmap: None,
            internal: Mutex::new(SegmentInternal {
                fd: None,
                block_written: 0,
                segment_written: 0,
                buffer: Vec::new(),
            }),
        }
    }

    pub(crate) fn open_mmap(path: PathBuf) -> Result<Self> {
        if is_empty_file(&path) {
            return Ok(Self::open_read_only(path));
        }
        let fd = File::open(&path)?;
        let mmap = unsafe { memmap::MmapOptions::new().map(&fd)? };
        Ok(Self {
            mutable: false,
            path,
            mmap: Some(RwLock::new(mmap)),
            internal: Mutex::new(SegmentInternal {
                fd: Some(fd),
                block_written: 0,
                segment_written: 0,
                buffer: Vec::new(),
            }),
        })
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.path.as_path().to_owned()
    }

    pub(crate) fn name(&self) -> String {
        os_str_to_string(self.path.file_stem())
    }

    pub(crate) fn index(&self) -> u64 {
        self.name().parse::<u64>().unwrap()
    }

    // create is the only way to get a mutable segment
    pub(crate) fn create(dir: &PathBuf, index: u64, ext: &str) -> Result<Self> {
        let filename = format!("{}.{}", index, ext);
        let path = dir.join(filename);
        let fd: File = File::create_new(&path)?;
        Ok(Self {
            mutable: true,
            path,
            mmap: None,
            internal: Mutex::new(SegmentInternal {
                fd: Some(fd),
                block_written: 0,
                segment_written: 0,
                buffer: Vec::new(),
            }),
        })
    }

    pub(crate) fn write(&self, key: &[u8], value: &[u8], flag: u8) -> Result<WriteResult> {
        if !self.mutable {
            return Err(anyhow!("segment is immutable"));
        }
        let internal = &mut *(self.internal.lock().unwrap());
        let fd = internal.fd.as_mut().unwrap();

        // encode key and value length
        let key_len_encoding = encode_varint_to_vec(key.len() as u64)?;
        let value_len_encoding = encode_varint_to_vec(value.len() as u64)?;
        let header_len = (key_len_encoding.len() + value_len_encoding.len() + 1) as u64;
        // let record_len = (header_len + value.len() as u64 + 4) as u64;

        // padding if necessary
        if header_len + internal.block_written > BLOCK_BYTES {
            // padding the rest of block
            // internal.block_written may be greater or equal with MAX_BLOCK_BYTES
            if BLOCK_BYTES - internal.block_written > 0 {
                let mut padding = vec![0; BLOCK_BYTES as usize - internal.block_written as usize];
                padding[0] = FLAG_PADDING;
                fd.write_all(&padding)?;
                internal.segment_written += padding.len() as u64;
            }
            // new block
            internal.block_written = 0;
        }

        let crc = Crc::<u32>::new(&CRC_CONFIG);
        let mut digest = crc.digest();
        digest.update(&key);
        digest.update(&value);
        let checksum = digest.finalize().to_le_bytes();
        // write record
        let begin_offset = internal.segment_written;
        internal.buffer.clear();
        internal.buffer.push(flag);
        internal.buffer.extend(key_len_encoding);
        internal.buffer.extend(value_len_encoding);
        internal.buffer.extend(key);
        internal.buffer.extend(value);
        internal.buffer.extend(checksum);
        let written = fd.write(internal.buffer.as_slice())?;
        internal.block_written += written as u64;
        internal.block_written %= BLOCK_BYTES;
        internal.segment_written += written as u64;
        let is_segment_full = internal.segment_written >= MAX_SEGMENT_BYTES;
        return Ok(WriteResult {
            is_segment_full,
            begin_offset,
        });
    }

    pub(crate) fn read_at(&self, offset: u64) -> Result<Record> {
        if self.mmap.is_some() {
            self.read_at_mmap(offset)
        } else {
            self.read_at_fd(offset)
        }
    }

    pub(crate) fn read_at_mmap(&self, offset: u64) -> Result<Record> {
        let mut offset: usize = offset as usize;
        let mmap = &*(self.mmap.as_ref().unwrap().read().unwrap());
        let flag = if let Some(f) = mmap.get(offset as usize) {
            f.to_owned()
        } else {
            return Err(anyhow!("reach end of file"));
        };
        offset += 1;
        if flag & FLAG_PADDING > 0 {
            return Ok(Record {
                key: Bytes::new(),
                value: Bytes::new(),
                flag: flag,
            });
        }
        let key_len = decode_varint_from_mmap(mmap, &mut offset)? as usize;
        let value_len = decode_varint_from_mmap(mmap, &mut offset)? as usize;
        let key: Vec<u8> = if let Some(slice) = mmap.get(offset..offset + key_len) {
            slice.to_vec()
        } else {
            return Err(anyhow!("reach end of file"));
        };
        offset += key_len;
        let value: Vec<u8> = if let Some(slice) = mmap.get(offset..offset + value_len) {
            slice.to_vec()
        } else {
            return Err(anyhow!("reach end of file"));
        };
        offset += value_len;
        Ok(Record {
            key: Bytes::from(key),
            value: Bytes::from(value),
            flag,
        })
    }

    pub(crate) fn read_at_fd(&self, offset: u64) -> Result<Record> {
        let internal = &mut *(self.internal.lock().unwrap());
        let fd = if let Some(fd) = internal.fd.as_mut() {
            fd
        } else {
            let fd = File::open(&self.path)?;
            internal.fd = Some(fd);
            internal.fd.as_mut().unwrap()
        };
        let mut flag_buffer = [0u8; 1];
        let n = fd.read_at(&mut flag_buffer, offset).unwrap();
        if n == 0 {
            // reach end of file
            return Err(anyhow!("reach end of file"));
        }
        // read flag
        let flag = flag_buffer[0];
        if flag & FLAG_PADDING > 0 {
            // it is a padding, move to next block
            return Ok(Record {
                key: Bytes::new(),
                value: Bytes::new(),
                flag: flag,
            });
        }
        // move to startof key_len_encoding
        fd.seek(SeekFrom::Start(offset + 1))?;

        // read length
        let (key_len, _) = decode_varint(fd)?;
        let (value_len, _) = decode_varint(fd)?;

        // read key
        internal.buffer.resize(key_len as usize, 0);
        fd.read_exact(&mut internal.buffer).unwrap();
        let key = internal.buffer.clone();

        // read value
        internal.buffer.resize(value_len as usize, 0);
        fd.read_exact(&mut internal.buffer).unwrap();
        let value = internal.buffer.clone();

        Ok(Record {
            key: Bytes::from(key),
            value: Bytes::from(value),
            flag,
        })
    }

    pub(crate) fn iter(&self) -> SegmentIter<'_> {
        SegmentIter::new(self, false)
    }

    pub(crate) fn iter_with_value(&self) -> SegmentIter<'_> {
        SegmentIter::new(self, true)
    }
}

pub(crate) struct SegmentIter<'a> {
    segment: &'a Segment,
    offset: u64,
    buffer: Vec<u8>,
    with_value: bool,
}

fn next_block_offset(offset: u64) -> u64 {
    if offset % BLOCK_BYTES == 0 {
        // if offset is start of block, move to next
        offset + BLOCK_BYTES
    } else {
        ((offset / BLOCK_BYTES) + 1) * BLOCK_BYTES
    }
}

impl<'a> Iterator for SegmentIter<'a> {
    type Item = RecordIndex;

    fn next(&mut self) -> Option<Self::Item> {
        let segment = self.segment;
        let internal = &mut *(segment.internal.lock().unwrap());
        let fd = if let Some(fd) = internal.fd.as_mut() {
            fd
        } else {
            let fd = File::open(&segment.path).unwrap();
            internal.fd = Some(fd);
            internal.fd.as_mut().unwrap()
        };
        let mut record_offset = self.offset;
        let mut flag_buffer = [0u8; 1];
        let n = fd.read_at(&mut flag_buffer, self.offset).unwrap();
        if n == 0 {
            // reach end of file
            return None;
        }
        // read flag
        let mut flag = flag_buffer[0];
        if flag & FLAG_PADDING > 0 {
            // it is a padding, move to next block
            self.offset = next_block_offset(self.offset);
            // now self.offset is at beginning of record, read flag of record
            record_offset = self.offset;
            if let Err(e) = fd.seek(SeekFrom::Start(self.offset)) {
                panic!("seek err: {:?}", e)
            }
            let n = fd.read(&mut flag_buffer).unwrap();
            if n == 0 {
                // reach end of file
                return None;
            }
            self.offset += n as u64;
            // read flag
            flag = flag_buffer[0];
        } else {
            self.offset += 1; // move offset to first byte of key length
            if let Err(e) = fd.seek(SeekFrom::Start(self.offset)) {
                panic!("seek err: {:?}", e)
            }
        }

        // read key len
        let key_len_result = decode_varint(fd);
        if key_len_result.is_err() {
            panic!("{:?}", key_len_result.unwrap_err())
        }
        let (key_len, n) = key_len_result.unwrap();
        self.offset += n;

        // read value len
        let value_len_result = decode_varint(fd);
        if value_len_result.is_err() {
            panic!("{:?}", value_len_result.unwrap_err())
        }
        let (value_len, n) = value_len_result.unwrap();
        self.offset += n;

        // read key
        self.buffer.resize(key_len as usize, 0);
        fd.read_exact(&mut self.buffer).unwrap();
        self.offset += key_len;
        let key = Bytes::from(self.buffer.clone());

        // read value
        let value: Option<Bytes> = if self.with_value {
            self.buffer.resize(value_len as usize, 0);
            fd.read_exact(&mut self.buffer).unwrap();
            self.offset += value_len;
            Some(Bytes::from(self.buffer.clone()))
        } else {
            self.offset += value_len;
            None
        };
        // skip crc
        self.offset += 4;

        Some(RecordIndex {
            segment: segment.name(),
            key,
            offset: record_offset,
            flag,
            value,
        })
    }
}

impl<'a> SegmentIter<'a> {
    fn new(segment: &'a Segment, with_value: bool) -> Self {
        SegmentIter {
            segment: segment,
            offset: 0,
            buffer: Vec::new(),
            with_value,
        }
    }
}
