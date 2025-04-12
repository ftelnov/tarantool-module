use super::{utils, Error};
use std::cell::Cell;
use std::ffi::{CString, NulError};
use std::fmt::Display;
use std::future::{self};
use std::mem::{self, MaybeUninit};
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{io, marker};

#[cfg(feature = "async-std")]
use async_std::io::{Read as AsyncRead, Write as AsyncWrite};
#[cfg(not(feature = "async-std"))]
use futures::{AsyncRead, AsyncWrite};
use libc::c_int;

use super::stream::TcpStream;
use super::utils::*;
use crate::ffi::tarantool::{self as ffi};
use crate::fiber;
use crate::fiber::r#async::context::ContextExt;
use crate::fiber::r#async::timeout::{self, IntoTimeout};
use crate::time::Instant;

pub struct TcpListener {
    connections_stream: TcpStream,
}

impl TcpListener {
    pub fn bind(host: &str, port: u16) -> Result<Self, Error> {
        let mut last_error = None;
        for addr in resolve_addr(host, port, Duration::MAX.as_secs_f64())?.into_iter() {
            match Self::bind_single((&addr).into()) {
                Ok(stream) => {
                    return Ok(stream);
                }
                Err(e) => last_error = Some(e),
            }
        }
        let Some(error) = last_error else {
            return Err(Error::ResolveAddress(format!("{host}:{port}")));
        };
        Err(Error::Bind {
            error,
            address: format!("{host}:{port}"),
        })
    }

    fn bind_single(addr_info: AddrInfo<'_>) -> std::io::Result<Self> {
        // SAFETY: `addr_info` is valid and used according to its lifetime.
        let fd = unsafe { bind_socket(&addr_info)? };
        // Convert the socket into a raw file descriptor.
        let raw_fd = fd.into_raw_fd();

        cvt(unsafe { libc::listen(raw_fd, libc::SOMAXCONN) })?;

        // Create the instance from the raw file descriptor.
        Ok(Self {
            connections_stream: raw_fd.into(),
        })
    }

    pub async fn accept(&self) -> Result<TcpStream, Error> {
        let raw_fd = self.connections_stream.fd()?;
        let f = future::poll_fn(|cx| {
            if let Err(error) = check_socket_error(&raw_fd) {
                // SAFETY: this fd is still valid and was not closed.
                unsafe { AutoCloseFd::from_raw_fd(raw_fd) };
                return Poll::Ready(Err(Error::Accept { error }));
            }

            match utils::accept(raw_fd) {
                Ok(raw_fd) => return Poll::Ready(Ok(raw_fd.into())),
                Err(error) => {
                    if error.kind() == io::ErrorKind::WouldBlock {
                        // SAFETY: safe as long as this future is executed by `fiber::block_on` async executor.
                        unsafe {
                            ContextExt::set_coio_wait(cx, raw_fd, ffi::CoIOFlags::READ);
                        }
                        return Poll::Pending;
                    }
                    return Poll::Ready(Err(Error::Accept { error }));
                }
            };
        });
        f.await
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use futures::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    const _10_SEC: Duration = Duration::from_secs(10);
    const _0_SEC: Duration = Duration::from_secs(0);

    #[crate::test(tarantool = "crate")]
    fn bind() {
        let _ = TcpListener::bind("localhost", 0).unwrap();
    }

    #[crate::test(tarantool = "crate")]
    async fn bind_accept_receive() {
        let data = [1; 128];

        let handle = fiber::start_async(async {
            let listener = TcpListener::bind("localhost", 18899).unwrap();
            let mut buf = vec![0; 128];
            let mut read_stream = listener.accept().await.unwrap();

            read_stream
                .read_exact(&mut buf)
                .timeout(_10_SEC)
                .await
                .unwrap();

            assert_eq!(buf, data);
        });

        let mut peer = TcpStream::connect_async("localhost", 18899).await.unwrap();
        peer.write_all(&data).await.unwrap();

        handle.cancel();
        handle.join();
    }
}
