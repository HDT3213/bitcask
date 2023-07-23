use std::path::{PathBuf};

use anyhow::{Ok, Result};

use crate::{
    storage::{directory::Directory, segment::Segment, Bytes, HINT_EXT_NAME},
    utils::utils::file_exists,
};

use super::{index::Index, merge::MERGE_FINISH_FILENAME};

#[derive(Debug, Clone)]
pub struct Options {
    mmap: bool,
}

impl Options {
    pub fn default() -> Self {
        Options {
            mmap: true,
        }
    }

    pub fn mmap(mut self, enable: bool) -> Self {
        self.mmap = enable;
        self
    }
}

pub struct Database {
    pub(super) root_dir: PathBuf,
    pub(super) index: Index,
    pub(super) storage: Directory,
}

impl Database {
    pub(super) fn get_merge_dir(root_dir: &PathBuf) -> PathBuf {
        root_dir.join(PathBuf::from("merged"))
    }

    pub(super) fn get_data_dir(root_dir: &PathBuf) -> PathBuf {
        root_dir.join(PathBuf::from("data"))
    }

    pub fn open(dir: &str, options: Options) -> Result<Self> {
        let root_dir = PathBuf::from(dir);
        let data_dir = Self::get_data_dir(&root_dir);
        let mut index = Index::new();
        Self::try_load_merged(&root_dir)?;
        std::fs::create_dir_all(&data_dir)?;
        let storage = Directory::open(data_dir.to_str().unwrap(), options.mmap)?;
        // bug fix: hint file exists but merged dir not exists
        Self::load_index(&mut index, &data_dir, &storage)?;
        Ok(Self {
            root_dir,
            index,
            storage,
        })
    }

    pub fn write(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let idx = self.storage.write(key, value, 0)?;
        self.index.set(idx)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.storage.write(key, &[], crate::storage::FLAG_DELETED)?;
        self.index.delete(&Bytes::from(key.to_vec()))?;
        Ok(())
    }

    pub fn read(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(idx) = self.index.get(key) {
            let record = self.storage.read_at(&idx)?;
            return Ok(Some(record.value));
        }
        Ok(None)
    }

    pub(super) fn load_index(
        index: &mut Index,
        data_dir: &PathBuf,
        directory: &Directory,
    ) -> Result<()> {
        let map = &mut *(index.map.write().unwrap());
        let hint_file_path = data_dir.join(format!("{}.{}", 1, HINT_EXT_NAME));
        let merge_finish_path = data_dir.join(MERGE_FINISH_FILENAME);
        let max_merged_segment: u64;
        if file_exists(&hint_file_path) {
            let merge_finish_file = std::fs::read_to_string(&merge_finish_path)?;
            max_merged_segment = merge_finish_file.trim().parse::<u64>()?;
            let hint_file = Segment::open_read_only(hint_file_path);
                for hint_index in hint_file.iter_with_value() {
                    let record_index =
                        Self::decode_record_index(hint_index.key.clone(), hint_index.value.unwrap())?;
                    map.insert(record_index.key.clone(), record_index);
                }
        } else {
            max_merged_segment = 0;
        }
        
        
        let internal = directory.internal.read().unwrap();
        for (_, segment) in internal.old_segments.iter() {
            if segment.index() <= max_merged_segment {
                continue;
            }
            for record_index in segment.iter() {
                if record_index.is_deleted() {
                    map.remove(&record_index.key);
                } else {
                    map.insert(record_index.key.clone(), record_index);
                }
            }
        }
        Ok(())
    }
}
