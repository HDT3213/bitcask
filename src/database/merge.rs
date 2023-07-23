use std::{
    collections::{BTreeMap},
    ffi::OsStr,
    fs,
    path::{PathBuf},
};

use super::database::Database;
use crate::{
    storage::{segment::Segment, Bytes, RecordIndex, HINT_EXT_NAME, SEG_EXT_NAME},
    utils::utils::{dir_exists, file_exists},
};
use anyhow::{anyhow, Result};
use std::io::prelude::*;

pub(crate) static MERGE_FINISH_FILENAME: &str = "merge-finish";

pub(super) struct LoadMerged {
    pub(super) max_merged_segment: u64,
    pub(super) hint_file: Option<PathBuf>,
}

impl Database {
    pub fn merge(&self) -> Result<()> {
        // load record index
        let preparation = self.storage.prepare_merge()?;
        if preparation.to_merge.is_empty() {
            return Ok(());
        }
        let mut records: BTreeMap<Bytes, RecordIndex> = BTreeMap::new();
        let mut segments: BTreeMap<String, Segment> = BTreeMap::new();
        for path in preparation.to_merge.iter() {
            let seg = Segment::open_read_only(path.to_owned());
            for ri in seg.iter() {
                if !ri.is_deleted() {
                    records.insert(ri.key.clone(), ri);
                }
            }
            segments.insert(seg.name(), seg);
        }
        let merge_dir = Self::get_merge_dir(&self.root_dir);
        // remove former merged data
        let _ = std::fs::remove_dir_all(&merge_dir);
        std::fs::create_dir_all(&merge_dir)?;

        // write to new segments
        let mut index: u64 = 1;
        let mut active_segment = Segment::create(&merge_dir, index, SEG_EXT_NAME)?;
        let hint_file = Segment::create(&merge_dir, 1, HINT_EXT_NAME)?;
        let mut buf: Vec<u8> = Vec::new();
        for (_, record_index) in records.iter() {
            if let Some(seg) = segments.get(record_index.segment.as_str()) {
                let record = seg.read_at(record_index.offset)?;
                let write_result = active_segment.write(
                    record.key.as_slice(),
                    record.value.as_slice(),
                    record.flag,
                )?;
                let hint_record = RecordIndex {
                    key: record_index.key.clone(),
                    segment: active_segment.name(),
                    flag: 0,
                    offset: write_result.begin_offset,
                    value: None,
                };
                Self::encode_record_index(&mut buf, &hint_record);
                // use only one hint file, ignore is_segment_full
                hint_file.write(record.key.as_slice(), buf.as_slice(), 0)?;
                if write_result.is_segment_full {
                    index += 1;
                    active_segment = Segment::create(&merge_dir, index, SEG_EXT_NAME)?
                }
            } else {
                // unreachable
                return Err(anyhow!("segment not found"));
            }
        }

        // write merge finish file into
        let merge_finish_path = Self::get_merge_dir(&self.root_dir).join(MERGE_FINISH_FILENAME);
        let mut merge_finish_file = std::fs::File::create(&merge_finish_path)?;
        let max_merged_segment_name = segments.last_key_value().unwrap().1.name();
        merge_finish_file.write(max_merged_segment_name.as_bytes())?;
        Ok(())
    }

    pub(super) fn try_load_merged(root_path: &PathBuf) -> Result<()> {
        let merge_dir = Self::get_merge_dir(root_path);
        let data_dir = Self::get_data_dir(root_path);
        if !dir_exists(merge_dir.as_path()) {
            // merge dir not found
            return Ok(());
        }
        let merge_finish_path = merge_dir.join(MERGE_FINISH_FILENAME);
        if !file_exists(merge_finish_path.as_path()) {
            // merge interrupted, remove data
            let _ = fs::remove_dir(merge_dir.as_path());
            return Ok(());
        }

        // remove merged segments
        // If this process is interrupted, it will continue to delete old segments on the next startup because the merged finish file is still exists
        let merge_finish_file = fs::read_to_string(&merge_finish_path)?;
        let max_merged_segment = merge_finish_file.trim().parse::<u64>()?;
        for i in 1..(max_merged_segment + 1) {
            let merged_segment_name = format!("{}.{}", i, SEG_EXT_NAME);
            let merged_path = data_dir.join(merged_segment_name);
            fs::remove_file(merged_path)?;
        }

        // copy merged segments to data dir
        // The maximum index of merged segments must be less than or equal to deleted segments
        // If this process is interrupted, it will continue to copy merged segments on the next startup because the merged directory is still complete
        for e in fs::read_dir(merge_dir.as_path())? {
            if let Ok(entry) = e {
                let p = entry.path();
                if p.is_file() && p.extension() == Some(OsStr::new(SEG_EXT_NAME)) {
                    let target_path = data_dir.join(p.file_name().unwrap());
                    fs::copy(p.as_path(), target_path.as_path())?;
                }
            }
        }

        // copy hint file
        let hint_filename = format!("{}.{}", 1, HINT_EXT_NAME);
        let src_hint_file = merge_dir.join(hint_filename.as_str());
        let mut hint_file: Option<PathBuf> = None;
        if file_exists(&src_hint_file) {
            let target_path = data_dir.join(hint_filename);
            fs::copy(src_hint_file.as_path(), target_path.as_path())?;
            hint_file = Some(target_path);
        }

        // copy merge finish file
        let target_merge_finish_path = data_dir.join(MERGE_FINISH_FILENAME);
        fs::copy(merge_finish_path, target_merge_finish_path)?;
        
        // The data dir is complete now, it is safe to remove merge dir
        fs::remove_dir_all(merge_dir.as_path())?;
        Ok(())
    }

    // encode segment name and offset to bytes for hint file
    pub(super) fn encode_record_index(buf: &mut Vec<u8>, index: &RecordIndex) {
        buf.clear();
        buf.extend_from_slice(index.segment.as_bytes());
        buf.push(b'\0'); // separator
        buf.extend_from_slice(index.offset.to_le_bytes().as_slice());
    }

    pub(super) fn decode_record_index(key: Bytes, hint_value: Bytes) -> Result<RecordIndex> {
        let segment: String;
        let offset: u64;
        match hint_value.as_slice().iter().position(|&x| x == 0) {
            Some(pivot) => {
                let seg_bytes = hint_value.as_slice()[..pivot].to_vec();
                segment = String::from_utf8(seg_bytes)?;
                offset = u64::from_le_bytes(hint_value.as_slice()[pivot + 1..].try_into().unwrap());
            }
            None => {
                return Err(anyhow!("pivot not found in hint record"));
            }
        };
        Ok(RecordIndex {
            key: key,
            segment: segment,
            flag: 0,
            offset: offset,
            value: None,
        })
    }
}
