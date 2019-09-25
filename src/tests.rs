#[cfg(test)]

extern crate test;

use futures_cpupool::{CpuPool};

#[allow(dead_code)]
const TEST_TEMPORARY_DIR: &'static str = "./test_tmp/";

#[allow(dead_code)]
static TEST_BUFFER_SIZE: usize = 8 * 1024;

lazy_static! {
    static ref TEST_CPU_POOL: CpuPool = CpuPool::new(1);
}


#[test]
fn it_write() {
    use futures::Future;
    use std::io::Read;
    use super::*;

    std::fs::create_dir_all(TEST_TEMPORARY_DIR).unwrap();

    let test_file_path: std::path::PathBuf = format!("{}it_write.txt", TEST_TEMPORARY_DIR).into();

    let async_file_write = AsyncFileWrite::from_std(
        &TEST_CPU_POOL,
        std::fs::File::create(&test_file_path).unwrap(),
        TEST_BUFFER_SIZE,
    );

    tokio::io::write_all(async_file_write, b"Hello")
        .and_then(|(async_file_write, _)| tokio::io::write_all(async_file_write, b" "))
        .and_then(|(async_file_write, _)| tokio::io::write_all(async_file_write, b"world!"))
        .wait().unwrap();

    let mut data: Vec<u8> = Vec::new();
    std::fs::File::open(&test_file_path).unwrap()
        .read_to_end(&mut data).unwrap();

    assert_eq!(data, b"Hello world!");

    std::fs::remove_file(test_file_path).unwrap();
}


#[test]
fn it_write_long() {
    use futures::Future;
    use std::io::Read;
    use super::*;

    std::fs::create_dir_all(TEST_TEMPORARY_DIR).unwrap();

    let test_file_path: std::path::PathBuf = format!("{}it_write_long.txt", TEST_TEMPORARY_DIR).into();

    {
        let mut async_file_write = AsyncFileWrite::from_std(
            &TEST_CPU_POOL,
            std::fs::File::create(&test_file_path).unwrap(),
            TEST_BUFFER_SIZE,
        );
        for i in 0..10 {
            async_file_write = tokio::io::write_all(async_file_write, vec![i + 1; 4096]).wait().unwrap().0;
        }
    }

    let mut data: Vec<u8> = Vec::new();
    std::fs::File::open(&test_file_path).unwrap()
        .read_to_end(&mut data).unwrap();

    assert_eq!(data.len(), 4096 * 10);
    assert_eq!(&data[..4096], &[1u8; 4096][..]);
    assert_eq!(&data[4096..8192], &[2u8; 4096][..]);

    std::fs::remove_file(test_file_path).unwrap();
}


#[test]
fn it_sync() {
    use std::io::Read;
    use futures::{Future, Sink};
    use super::*;

    std::fs::create_dir_all(TEST_TEMPORARY_DIR).unwrap();

    let test_file_path: std::path::PathBuf = format!("{}it_sync.txt", TEST_TEMPORARY_DIR).into();

    let bytes = futures::stream::iter_ok::<_, std::io::Error>(
        vec!["Hello", " ", "world!"]
            .into_iter()
            .map(|v| v.into()),
    );

    let _ = AsyncFileSink::from_std(
        &TEST_CPU_POOL,
        std::fs::File::create(&test_file_path).unwrap(),
    ).send_all(bytes).wait().unwrap();

    let mut data: Vec<u8> = Vec::new();
    std::fs::File::open(&test_file_path).unwrap()
        .read_to_end(&mut data).unwrap();

    assert_eq!(data, b"Hello world!");

    std::fs::remove_file(test_file_path).unwrap();
}

#[test]
fn it_sync_long() {
    use futures::Future;
    use std::io::Read;
    use futures::Sink;
    use super::*;

    std::fs::create_dir_all(TEST_TEMPORARY_DIR).unwrap();

    let test_file_path: std::path::PathBuf = format!("{}it_sync_long.txt", TEST_TEMPORARY_DIR).into();

    {
        let mut async_file_sink = AsyncFileSink::from_std(
            &TEST_CPU_POOL,
            std::fs::File::create(&test_file_path).unwrap(),
        );
        for i in 0..10 {
            async_file_sink = async_file_sink.send(vec![i + 1; 4096].into()).wait().unwrap();
        }
    }

    let mut data: Vec<u8> = Vec::new();
    std::fs::File::open(&test_file_path).unwrap()
        .read_to_end(&mut data).unwrap();

    assert_eq!(data.len(), 4096 * 10);
    assert_eq!(&data[..4096], &[1u8; 4096][..]);
    assert_eq!(&data[4096..8192], &[2u8; 4096][..]);

    std::fs::remove_file(test_file_path).unwrap();
}

#[test]
fn it_read() {
    use futures::Future;
    use super::*;

    let async_file_read = AsyncFileRead::from_std(
        &TEST_CPU_POOL,
        std::fs::File::open("./assets/hello.txt").unwrap(),
        TEST_BUFFER_SIZE,
    );
    let (_, output) = tokio::io::read_to_end(async_file_read, Vec::new()).wait().unwrap();

    assert_eq!(output, b"Hello world!\n");
}


#[test]
fn it_stream() {
    use futures::stream::Stream;
    use futures::future;
    use super::*;

    let async_file_stream = AsyncFileStream::from_std(
        &TEST_CPU_POOL,
        std::fs::File::open("./assets/hello.txt").unwrap(),
        TEST_BUFFER_SIZE,
    );
    let output = async_file_stream.fold(Vec::new(), |mut output: Vec<u8>, chunk| {
        output.extend_from_slice(&chunk[..]);
        future::ok::<_, std::io::Error>(output)
    }).wait().unwrap();

    assert_eq!(output, b"Hello world!\n");
}
