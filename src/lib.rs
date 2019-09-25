#![feature(async_await)]
#![feature(test)]
#![feature(rustc_private)]

#[macro_use] extern crate lazy_static;
extern crate futures;
extern crate futures_cpupool;
extern crate tokio;
extern crate bytes;

use futures::{Poll, Future, Async, AsyncSink};
use futures_cpupool::{CpuPool, CpuFuture};
use std::sync::{Arc, RwLock};
use std::convert::AsRef;
use std::io::{Write, Read};
use std::convert::TryFrom;
use bytes::{Bytes};

mod tests;

static DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

lazy_static! {
    /// Стандартая библиотека Rust не предоставляет API для асинхронной работы с файловой системой,
    /// основанных на механизмах epoll - вероятно, тому есть достаточная мотивация,
    /// связанная с неоправданностью по производительности и усложнением конструкций.
    /// Если сильно понадобится можно посмотреть в libc.
    ///
    /// Асинхронное API, предоставляеемое tokio fs, также являются довольно странной оберткой,
    /// выполняющей в пуле потоков блокирующие API из станадратной библиотеки.
    ///
    /// Гипотетически количество потоков для блокирующих операций с файловой системой
    /// не будет играть значительной роли,
    /// ибо сколько бы ядер процессора ни было, они не смогут работать параллельно, так как
    /// в один момент времени только одна операция может выполняться на одном физическом носителе,
    /// и все потоки будут ожидать завершения текущей операции ввода-вывода.
    ///
    /// Также, даже если вы работаете с несколькми физическими носителями,
    /// использование большего пула не гарантирует, параллельное выполнение,
    /// в связи с тем, что все потоки из этого пула в какой-то момент времени могут "застрять"
    /// в ожидающей очереди операций на одном носителе.
    ///
    /// Поэтому лучше отвести отдельный пул потоков для каждого устройства.
    /// Например, если вы точно знаете, что путь к файлу, в который вы записываете,
    /// соответствует отдельному физическому носителю:
    ///
    /// cpu_pool_home.spawn_fn(|| std::fs::File::create("/home/any_path/any_file.any_ext".into()))
    ///     - в директорию "home" монтирован физический накопитель SSD 60Gb
    ///
    /// cpu_pool_data.spawn_fn(|| std::fs::File::create("/data/any_path/any_file.any_ext".into()))
    ///     - в директорию "data" монтирован физический накопитель HDD 500Gb
    ///
    /// В данном случае, 2 потока позволяют заполнять простой очереди вызовов к ядру системы,
    /// между короткими промежутками времени, в которые потоки выполняют инструкции
    /// неопосредственно не связанные с вводом-выводом. Например, принимают следующее сообщение из канала.
    pub static ref DEFAULT_CPU_POOL: CpuPool = CpuPool::new(2);
}


// AsyncFileWrite

enum AsyncFileWriteState {
    Write(CpuFuture<(std::fs::File, usize), std::io::Error>),
    Flush(CpuFuture<std::fs::File, std::io::Error>),
    Ready(std::fs::File),
    Swapping,
}

/// Структура для асинхронной записи файла
pub struct AsyncFileWrite {
    cpu_pool: &'static CpuPool,
    state: AsyncFileWriteState,
    buf: Arc<RwLock<Vec<u8>>>,
}
impl AsyncFileWrite {

    #[inline]
    pub fn from_std (cpu_pool: &'static CpuPool, file: std::fs::File, buffer_size: usize) -> AsyncFileWrite {
        AsyncFileWrite {
            cpu_pool,
            state: AsyncFileWriteState::Ready(file),
            buf: Arc::new(RwLock::new(Vec::with_capacity(buffer_size)))
        }
    }
}

impl std::io::Write for AsyncFileWrite {
    fn write(&mut self, src: &[u8]) -> std::io::Result<usize> {
        loop {
            match self.state {
                AsyncFileWriteState::Write(ref mut future) => {
                    match future.poll()? {
                        Async::Ready((file, size)) => {
                            self.state = AsyncFileWriteState::Ready(file);
                            return Ok(size);
                        },
                        _ => {
                            break;
                        }
                    }
                },
                AsyncFileWriteState::Ready(_) => {
                    if let AsyncFileWriteState::Ready(mut file) = std::mem::replace(&mut self.state, AsyncFileWriteState::Swapping) {
                        let buf = {
                            let mut buf = self.buf.write().unwrap();
                            buf.truncate(0);
                            let mut len = src.len();
                            let cap = buf.capacity();
                            if len > cap {
                                len = cap;
                            }
                            buf.extend_from_slice(&src[..len]);
                            self.buf.clone()
                        };
                        self.state = AsyncFileWriteState::Write(self.cpu_pool.spawn_fn(move || {
                            let size = file.write(&buf.read().unwrap()[..])?;
                            Ok((file, size))
                        }));
                    }
                },
                AsyncFileWriteState::Swapping => {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, "`File` instance already shutdown"));
                },
                _ => {
                    break;
                }
            };
        }

        Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "block"))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        loop {
            match self.state {
                AsyncFileWriteState::Flush(ref mut future) => {
                    match future.poll()? {
                        Async::Ready(file) => {
                            self.state = AsyncFileWriteState::Ready(file);
                            return Ok(());
                        },
                        _ => {
                            break;
                        }
                    }
                },
                AsyncFileWriteState::Ready(_) => {
                    if let AsyncFileWriteState::Ready(mut file) = std::mem::replace(&mut self.state, AsyncFileWriteState::Swapping) {
                        self.state = AsyncFileWriteState::Flush(self.cpu_pool.spawn_fn(|| {
                            let _ = file.flush()?;
                            Ok(file)
                        }));
                    }
                },
                AsyncFileWriteState::Swapping => {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, "`File` instance already shutdown"));
                },
                _ => {
                    break;
                }
            };
        }

        Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "`File` instance is blocked"))
    }
}

impl tokio::io::AsyncWrite for AsyncFileWrite {
    fn shutdown(&mut self) -> futures::Poll<(), std::io::Error> {
        Ok(Async::Ready(()))
    }
}
impl From<std::fs::File> for AsyncFileWrite {
    fn from(file: std::fs::File) -> Self {
        Self::from_std(&DEFAULT_CPU_POOL, file, DEFAULT_BUFFER_SIZE)
    }
}
impl TryFrom<AsyncFileWrite> for std::fs::File {
    type Error = std::io::Error;

    fn try_from(file: AsyncFileWrite) -> Result<Self, Self::Error> {
        match file.state {
            AsyncFileWriteState::Ready(file) => Ok(file),
            AsyncFileWriteState::Swapping => Err(std::io::Error::new(std::io::ErrorKind::Other, "`File` instance already shutdown")),
            _ => Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "`File` instance is blocked"))
        }
    }
}
impl std::fmt::Debug for AsyncFileWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("AsyncFileWrite").finish()
    }
}


// AsyncFileSink

enum AsyncFileSinkState {
    Write(CpuFuture<std::fs::File, std::io::Error>),
    Ready(std::fs::File),
    Swapping,
}

/// Структура для асинхронной записи файла
pub struct AsyncFileSink {
    cpu_pool: &'static CpuPool,
    state: AsyncFileSinkState,
}
impl AsyncFileSink {

    #[inline]
    pub fn from_std (cpu_pool: &'static CpuPool, file: std::fs::File) -> AsyncFileSink {
        AsyncFileSink {
            cpu_pool,
            state: AsyncFileSinkState::Ready(file),
        }
    }

}
impl futures::Sink for AsyncFileSink {
    type SinkItem = Bytes;
    type SinkError = std::io::Error;

    fn start_send(&mut self, item: Self::SinkItem) -> futures::StartSend<Self::SinkItem, Self::SinkError> {

        match self.state {
            AsyncFileSinkState::Write(ref mut future) => {
                match future.poll()? {
                    Async::Ready(mut file) => {
                        self.state = AsyncFileSinkState::Write(self.cpu_pool.spawn_fn(move || {
                            file.write_all(item.as_ref())?;
                            Ok(file)
                        }));
                        Ok(AsyncSink::Ready)
                    }
                    _ => Ok(AsyncSink::NotReady(item))
                }
            },
            AsyncFileSinkState::Ready(_) => {
                if let AsyncFileSinkState::Ready(mut file) = std::mem::replace(&mut self.state, AsyncFileSinkState::Swapping) {
                    self.state = AsyncFileSinkState::Write(self.cpu_pool.spawn_fn(move || {
                        file.write_all(item.as_ref())?;
                        Ok(file)
                    }));
                    return Ok(AsyncSink::Ready);
                }
                Ok(AsyncSink::NotReady(item))
            },
            AsyncFileSinkState::Swapping => Err(std::io::Error::new(std::io::ErrorKind::Other, "`File` instance already shutdown")),
        }
    }

    fn poll_complete(&mut self) -> futures::Poll<(), Self::SinkError> {
        match self.state {
            AsyncFileSinkState::Write(ref mut future) => {
                match future.poll()? {
                    Async::Ready(file) => {
                        self.state = AsyncFileSinkState::Ready(file);
                        Ok(Async::Ready(()))
                    },
                    _ => Ok(Async::NotReady)
                }
            },
            AsyncFileSinkState::Ready(_) => Ok(Async::Ready(())),
            AsyncFileSinkState::Swapping => Err(std::io::Error::new(std::io::ErrorKind::Other, "`File` instance already shutdown")),
        }
    }
}
impl From<std::fs::File> for AsyncFileSink {
    fn from(file: std::fs::File) -> Self {
        Self::from_std(&DEFAULT_CPU_POOL, file)
    }
}
impl TryFrom<AsyncFileSink> for std::fs::File {
    type Error = std::io::Error;

    fn try_from(file: AsyncFileSink) -> Result<Self, Self::Error> {
        match file.state {
            AsyncFileSinkState::Ready(file) => Ok(file),
            AsyncFileSinkState::Swapping => Err(std::io::Error::new(std::io::ErrorKind::Other, "`File` instance already shutdown")),
            _ => Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "`File` instance is blocked"))
        }
    }
}
impl std::fmt::Debug for AsyncFileSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("AsyncFileSink").finish()
    }
}


// AsyncFileRead

enum AsyncFileReadState {
    Read(CpuFuture<(std::fs::File, usize), std::io::Error>),
    Ready(std::fs::File),
    Swapping,
}

/// Структура для асинхронного чтения файла
pub struct AsyncFileRead {
    cpu_pool: &'static CpuPool,
    state: AsyncFileReadState,
    buf: Arc<RwLock<Vec<u8>>>
}
impl AsyncFileRead {
    #[inline]
    pub fn from_std (cpu_pool: &'static CpuPool, file: std::fs::File, buffer_size: usize) -> AsyncFileRead {
        let mut buf = Vec::with_capacity(buffer_size);
        unsafe {
            buf.set_len(buffer_size);
        }
        AsyncFileRead {
            cpu_pool,
            state: AsyncFileReadState::Ready(file),
            buf: Arc::new(RwLock::new(buf))
        }
    }
}


impl std::io::Read for AsyncFileRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.state {
                AsyncFileReadState::Read(ref mut future) => {
                    match future.poll()? {
                        Async::Ready((file, size)) => {
                            self.state = AsyncFileReadState::Ready(file);
                            buf[..size].clone_from_slice(&self.buf.read().unwrap()[..size]);
                            return Ok(size);
                        },
                        _ => {
                            break;
                        }
                    }
                },
                AsyncFileReadState::Ready(_) => {
                    if let AsyncFileReadState::Ready(mut file) = std::mem::replace(&mut self.state, AsyncFileReadState::Swapping) {
                        let mut len = buf.len();
                        {
                            let cap = self.buf.write().unwrap().capacity();
                            if len > cap {
                                len = cap;
                            }
                        }
                        let self_buf = self.buf.clone();

                        self.state = AsyncFileReadState::Read(self.cpu_pool.spawn_fn(move || {
                            let size = file.read(&mut self_buf.write().unwrap()[..len])?;
                            Ok((file, size))
                        }));
                    }
                },
                AsyncFileReadState::Swapping => {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, "`File` instance already shutdown"));
                }
            };
        }

        Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "block"))
    }
}
impl tokio::io::AsyncRead for AsyncFileRead {}


impl From<std::fs::File> for AsyncFileRead {
    fn from(file: std::fs::File) -> Self {
        Self::from_std(&DEFAULT_CPU_POOL, file, DEFAULT_BUFFER_SIZE)
    }
}
impl TryFrom<AsyncFileRead> for std::fs::File {
    type Error = std::io::Error;

    fn try_from(file: AsyncFileRead) -> Result<Self, Self::Error> {
        match file.state {
            AsyncFileReadState::Ready(file) => Ok(file),
            AsyncFileReadState::Swapping => Err(std::io::Error::new(std::io::ErrorKind::Other, "`File` instance already shutdown")),
            _ => Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "`File` instance is blocked"))
        }
    }
}
impl std::fmt::Debug for AsyncFileRead {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("AsyncFileRead").finish()
    }
}


// AsyncFileStream

enum AsyncFileStreamState {
    Read(CpuFuture<(std::fs::File, Vec<u8>), std::io::Error>),
    Ready(std::fs::File),
    Swapping,
}

/// Структура для асинхронного чтения файла
pub struct AsyncFileStream {
    cpu_pool: &'static CpuPool,
    state: AsyncFileStreamState,
    buffer_size: usize,
}
impl AsyncFileStream {
    #[inline]
    pub fn from_std (cpu_pool: &'static CpuPool, file: std::fs::File, buffer_size: usize) -> AsyncFileStream {
        AsyncFileStream {
            cpu_pool,
            state: AsyncFileStreamState::Ready(file),
            buffer_size
        }
    }
}
impl futures::stream::Stream for AsyncFileStream {
    type Item = Bytes;
    type Error = std::io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match self.state {
                AsyncFileStreamState::Read(ref mut future) => {
                    match future.poll()? {
                        Async::Ready((file, buf)) => {
                            self.state = AsyncFileStreamState::Ready(file);
                            return Ok(Async::Ready(
                                if buf.len() > 0 {
                                    Some(Bytes::from(buf))
                                } else {
                                    None
                                }
                            ));
                        },
                        _ => {
                            break;
                        }
                    }
                },
                AsyncFileStreamState::Ready(_) => {
                    if let AsyncFileStreamState::Ready(mut file) = std::mem::replace(&mut self.state, AsyncFileStreamState::Swapping) {
                        let buffer_size = self.buffer_size;
                        let mut buf: Vec<u8> = Vec::with_capacity(buffer_size);
                        unsafe {
                            buf.set_len(buffer_size);
                        }
                        self.state = AsyncFileStreamState::Read(self.cpu_pool.spawn_fn(move || {
                            let size = file.read(&mut buf[..buffer_size])?;
                            buf.truncate(size);
                            Ok((file, buf))
                        }));
                    }
                },
                AsyncFileStreamState::Swapping => {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, "`File` instance already shutdown"));
                }
            };
        }
        Ok(Async::NotReady)
    }
}
impl From<std::fs::File> for AsyncFileStream {
    fn from(file: std::fs::File) -> Self {
        Self::from_std(&DEFAULT_CPU_POOL, file, DEFAULT_BUFFER_SIZE)
    }
}
impl TryFrom<AsyncFileStream> for std::fs::File {
    type Error = std::io::Error;

    fn try_from(file: AsyncFileStream) -> Result<Self, Self::Error> {
        match file.state {
            AsyncFileStreamState::Ready(file) => Ok(file),
            AsyncFileStreamState::Swapping => Err(std::io::Error::new(std::io::ErrorKind::Other, "`File` instance already shutdown")),
            _ => Err(std::io::Error::new(std::io::ErrorKind::WouldBlock, "`File` instance is blocked"))
        }
    }
}
impl std::fmt::Debug for AsyncFileStream {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("AsyncFileStream").finish()
    }
}
