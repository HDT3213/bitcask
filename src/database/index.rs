use anyhow::{Ok, Result};
use std::{
    collections::BTreeMap, sync::RwLock,
};

use crate::storage::{Bytes, RecordIndex};

pub(super) struct Index {
    pub(super) map: RwLock<BTreeMap<Bytes, RecordIndex>>,
}

impl Index {
    pub(super) fn new() -> Self {
        Self {
            map: RwLock::new(BTreeMap::new()),
        }
    }

    pub(super) fn get(&self, key: &[u8]) -> Option<RecordIndex> {
        let map = self.map.read().unwrap();
        map.get(key).cloned()
    }

    pub(super) fn set(&mut self, record: RecordIndex) -> Result<()> {
        let mut map = self.map.write().unwrap();
        map.insert(record.key.clone(), record);
        Ok(())
    }

    pub(super) fn delete(&mut self, key: &Bytes) -> Result<()> {
        let mut map = self.map.write().unwrap();
        map.remove(key);
        Ok(())
    }
}
