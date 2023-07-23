#[cfg(test)]
mod tests {
    use crate::database::{
        database::{Database, Options},
    };
    use std::{
        path::PathBuf,
    };

    #[test]
    fn test_read_write_delete() {
        let dir_path = PathBuf::from("testdata");
        let _ = std::fs::remove_dir_all(&dir_path);
        std::fs::create_dir_all(&dir_path).unwrap();
        let mut cases: Vec<(String, String)> = Vec::new();
        for i in 0..10000 {
            cases.push((format!("{:016}", i), format!("v{:016}", i)));
        }
        {
            let mut database = Database::open("testdata", Options::default()).unwrap();
            for (key, value) in cases.iter() {
                database.write(key.as_bytes(), value.as_bytes()).unwrap();
            }
            for (key, value) in cases.iter() {
                let result = database.read(key.as_bytes()).unwrap();
                if result.is_none() {
                    panic!("record not found")
                }
                if result.unwrap().as_slice() != value.as_bytes() {
                    panic!("read returns wrong result")
                }
            }
        }
        {
            let mut database = Database::open("testdata", Options::default()).unwrap();
            for (key, value) in cases.iter() {
                let result = database.read(key.as_bytes()).unwrap();
                if result.is_none() {
                    panic!("record not found")
                }
                if result.unwrap().as_slice() != value.as_bytes() {
                    panic!("read returns wrong result")
                }
            }

            for (key, _) in cases.iter() {
                database.delete(key.as_bytes()).unwrap();
            }
            for (key, _) in cases.iter() {
                let result = database.read(key.as_bytes()).unwrap();
                if !result.is_none() {
                    panic!("record should be none")
                }
            }
        }
        {
            let database = Database::open("testdata", Options::default()).unwrap();
            for (key, _value) in cases.iter() {
                let result = database.read(key.as_bytes()).unwrap();
                if !result.is_none() {
                    panic!("record should be none")
                }
            }
        }
    }

    #[test]
    fn test_merge() {
        let dir_path = PathBuf::from("testdata");
        let _ = std::fs::remove_dir_all(&dir_path);
        std::fs::create_dir_all(&dir_path).unwrap();
        let mut cases: Vec<(String, String)> = Vec::new();
        for i in 0..1000 {
            cases.push((format!("{:016}", i), format!("v{:016}", i)));
        }
        {
            let mut database = Database::open("testdata", Options::default()).unwrap();
            for (key, value) in cases.iter() {
                database.write(key.as_bytes(), value.as_bytes()).unwrap();
            }
            for (key, value) in cases.iter() {
                database.write(key.as_bytes(), value.as_bytes()).unwrap();
            }
            database.merge().unwrap();
        }
        {
            let database = Database::open("testdata", Options::default()).unwrap();
            for (key, value) in cases.iter() {
                let result = database.read(key.as_bytes()).unwrap();
                if result.is_none() {
                    panic!("record not found")
                }
                if result.unwrap().as_slice() != value.as_bytes() {
                    panic!("read returns wrong result")
                }
            }
        }
    }
}
