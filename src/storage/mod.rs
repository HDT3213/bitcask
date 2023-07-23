use std::{borrow::Borrow, rc::Rc};

pub(crate) mod directory;
pub(crate) mod segment;

const FLAG_PADDING: u8 = 1;
pub(crate) const FLAG_DELETED: u8 = 1 << 1;
pub(crate) const SEG_EXT_NAME: &str = "seg";
pub(crate) const HINT_EXT_NAME: &str = "hint";

#[derive(Debug, Clone)]
pub(crate) struct RecordIndex {
    pub(crate) key: Bytes,
    pub(crate) segment: String,
    pub(crate) flag: u8,
    pub(crate) offset: u64,
    pub(crate) value: Option<Bytes>, // only is some in iter_with_value
}

impl RecordIndex {
    pub(crate) fn is_deleted(&self) -> bool {
        self.flag & FLAG_DELETED > 0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bytes {
    value: Rc<Vec<u8>>,
}

impl Bytes {
    pub(crate) fn new() -> Self {
        Bytes {
            value: Rc::new(Vec::new()),
        }
    }

    pub(crate) fn from(v: Vec<u8>) -> Self {
        Bytes { value: Rc::new(v) }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        (*self.value).as_slice()
    }
}

impl Borrow<[u8]> for Bytes {
    fn borrow(&self) -> &[u8] {
        &self.value
    }
}

impl std::fmt::Display for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes: Vec<u8> = (&*self.value).to_owned();
        write!(f, "{}", String::from_utf8(bytes).unwrap())
    }
}

#[derive(Debug)]
pub(crate) struct Record {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
    pub(crate) flag: u8,
}
