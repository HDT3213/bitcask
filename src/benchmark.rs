#[cfg(test)]
mod tests {
    use crate::database::{
        database::{Database, Options},
    };
    use std::{
        ops::Div,
        path::PathBuf,
        time::{Instant},
    };

    fn rand_string(length: usize) -> String {
        use rand::Rng;
        (0..length)
            .map(|_| rand::thread_rng().sample(rand::distributions::Alphanumeric))
            .map(char::from)
            .collect()
    }

    #[test]
    fn benchmark_tiny_objects() {
        let dir_path = PathBuf::from("testdata");
        let _ = std::fs::remove_dir_all(&dir_path);
        std::fs::create_dir_all(&dir_path).unwrap();
        const SIZE: usize = 100000;
        const KEY_LEN: usize = 16;
        const VALUE_LEN: usize = 100;
        let mut cases: Vec<(String, String)> = Vec::new();
        for i in 0..SIZE {
            cases.push((format!("{:016}", i), rand_string(VALUE_LEN)));
        }
        {
            let start_time = Instant::now();
            let mut database = Database::open("testdata", Options::default()).unwrap();
            for (key, value) in cases.iter() {
                database.write(key.as_bytes(), value.as_bytes()).unwrap();
            }
            let elapsed = Instant::elapsed(&start_time);
            let avg_elapsed = elapsed.div(SIZE as u32);
            println!(
                "random write {:?} ns/ops {:.3} ops/s",
                avg_elapsed.as_nanos(),
                1.0 / avg_elapsed.as_secs_f64()
            );
        }
        {
            use rand::seq::SliceRandom;
            cases.shuffle(&mut rand::thread_rng());
            let mut start_time = Instant::now();
            let database = Database::open("testdata", Options::default()).unwrap();
            let elapsed = Instant::elapsed(&start_time);
            let avg_elapsed = elapsed.div(SIZE as u32);
            println!("load  {:.3} records/s", 1.0 / avg_elapsed.as_secs_f64());
            start_time = Instant::now();
            for (key, value) in cases.iter() {
                let result = database.read(key.as_bytes()).unwrap();
                if result.is_none() {
                    panic!("record not found")
                }
                if result.unwrap().as_slice() != value.as_bytes() {
                    panic!("read returns wrong result")
                }
            }
            let elapsed = Instant::elapsed(&start_time);
            let avg_elapsed = elapsed.div(SIZE as u32);
            println!(
                "random read {:?} ns/ops {:.3} ops/s",
                avg_elapsed.as_nanos(),
                1.0 / avg_elapsed.as_secs_f64()
            );
        }
    }
}
