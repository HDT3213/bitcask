use std::{
    collections::{BTreeMap},
    ffi::OsStr,
    path::PathBuf,
    sync::RwLock,
};

use crate::utils::utils::os_str_to_string;
use anyhow::{anyhow, Result};

use super::{
    segment::{Segment, WriteResult},
    Bytes, Record, RecordIndex, SEG_EXT_NAME,
};

pub(crate) struct Directory {
    pub(crate) internal: RwLock<DirectoryInternal>,
}

pub(crate) struct DirectoryInternal {
    pub(crate) dir_path: PathBuf,
    pub(crate) active_segment: Segment,
    pub(crate) old_segments: BTreeMap<String, Segment>,
    pub(crate) use_mmap: bool,
}

pub(crate) struct MergePreparation {
    pub(crate) to_merge: Vec<PathBuf>,
}

impl Directory {
    pub(crate) fn open(dir: &str, use_mmap: bool) -> Result<Self> {
        let dir_path = PathBuf::from(dir);
        let read_dir = std::fs::read_dir(&dir_path)?;
        let mut old_segment_vec: Vec<Segment> = Vec::new();
        for e in read_dir {
            if let Ok(entry) = e {
                let p = entry.path();
                if p.is_file() && p.extension() == Some(OsStr::new(SEG_EXT_NAME)) {
                    let segment = if use_mmap {
                        Segment::open_mmap(p)?
                    } else {
                        Segment::open_read_only(p)
                    };
                    old_segment_vec.push(segment);
                }
            }
        }
        if old_segment_vec.is_empty() {
            return Self::new_directory(dir, use_mmap);
        }
        let last_file_stem = os_str_to_string(old_segment_vec.last().unwrap().path().file_stem());
        let last_file_index: usize = last_file_stem.parse()?;
        let active_segment_index = last_file_index as u64 + 1;
        let active_segment = Segment::create(&dir_path, active_segment_index, SEG_EXT_NAME)?;

        let old_segments: BTreeMap<String, Segment> =
            old_segment_vec.into_iter().map(|s| (s.name(), s)).collect();
        Ok(Directory {
            internal: RwLock::new(DirectoryInternal {
                dir_path,
                active_segment,
                old_segments,
                use_mmap,
            }),
        })
    }

    fn new_directory(dir: &str, use_mmap: bool) -> Result<Self> {
        let dir_path = PathBuf::from(dir);
        std::fs::create_dir_all(&dir_path)?;
        let active_segment_index: u64 = 1;
        let active_segment = Segment::create(&dir_path, active_segment_index, SEG_EXT_NAME)?;
        Ok(Directory {
            internal: RwLock::new(DirectoryInternal {
                dir_path,
                active_segment,
                old_segments: BTreeMap::new(),
                use_mmap
            }),
        })
    }

    pub(crate) fn prepare_merge(&self) -> Result<MergePreparation> {
        let internal = &mut *(self.internal.write().unwrap());
        Self::rotate_active_segment(internal)?;
        let to_merge = internal
            .old_segments
            .iter()
            .map(|(_, x)| x.path())
            .collect::<Vec<PathBuf>>();
        Ok(MergePreparation { to_merge })
    }

    pub(crate) fn read_at(&self, index: &RecordIndex) -> Result<Record> {
        let internal = self.internal.read().unwrap();
        if index.segment == internal.active_segment.name() {
            return internal.active_segment.read_at(index.offset);
        }
        if let Some(segment) = internal.old_segments.get(&index.segment) {
            return segment.read_at(index.offset);
        }
        Err(anyhow!("segment not found"))
    }

    pub(crate) fn write(&self, key: &[u8], value: &[u8], flag: u8) -> Result<RecordIndex> {
        let write_result: WriteResult;
        let current_active_segment: String;
        {
            // fields of directory will not be changed, read lock is enough
            let internal = self.internal.read().unwrap();
            write_result = internal.active_segment.write(key, value, flag)?;
            current_active_segment = internal.active_segment.name();
        }
        if write_result.is_segment_full {
            let internal = &mut *(self.internal.write().unwrap());
            if internal.active_segment.name() == current_active_segment {
                // check-lock-check
                Self::rotate_active_segment(internal)?;
            }
        }
        Ok(RecordIndex {
            key: Bytes::from(key.to_vec()),
            segment: current_active_segment,
            offset: write_result.begin_offset,
            flag,
            value: None,
        })
    }

    fn rotate_active_segment(internal: &mut DirectoryInternal) -> Result<()> {
        let old_segment_path = internal.dir_path.join(format!(
            "{}.{}",
            internal.active_segment.name(),
            SEG_EXT_NAME
        ));
        let new_index = internal.active_segment.index() + 1;
        let new_active_segment = Segment::create(&internal.dir_path, new_index, SEG_EXT_NAME)?;
        internal.active_segment = new_active_segment; // old segment should be dropped
        let old_active_segment = Segment::open_read_only(old_segment_path);
        internal
            .old_segments
            .insert(internal.active_segment.name(), old_active_segment);
        Ok(())
    }
}
