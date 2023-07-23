use std::ffi::OsStr;


pub(crate) fn os_str_to_string(src: Option<&OsStr>) -> String {
    src.and_then(|os_str| os_str.to_str())
        .unwrap_or_default()
        .to_string()
}

pub(crate) fn dir_exists<P: AsRef<std::path::Path>>(dir_path: P) -> bool {
    if let Ok(metadata) = std::fs::metadata(dir_path) {
        metadata.is_dir()
    } else {
        false
    }
}

pub(crate) fn file_exists<P: AsRef<std::path::Path>>(dir_path: P) -> bool {
    if let Ok(metadata) = std::fs::metadata(dir_path) {
        metadata.is_file()
    } else {
        false
    }
}

pub(crate) fn is_empty_file<P: AsRef<std::path::Path>>(path: P) -> bool {
    if let Ok(metadata) = std::fs::metadata(path) {
        return metadata.len() == 0;
    }
    false
}