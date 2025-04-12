use super::Error;
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

use crate::ffi::tarantool::{self as ffi};
use crate::fiber;
use crate::fiber::r#async::context::ContextExt;
use crate::fiber::r#async::timeout::{self, IntoTimeout};
use crate::time::Instant;

pub fn cvt(t: libc::c_int) -> io::Result<libc::c_int> {
    if t == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(t)
    }
}

/// A wrapper around a raw file descriptor, which automatically closes the
/// descriptor if dropped.
pub struct AutoCloseFd(RawFd);

impl AsRawFd for AutoCloseFd {
    #[inline(always)]
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl FromRawFd for AutoCloseFd {
    #[inline(always)]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self(fd)
    }
}

impl IntoRawFd for AutoCloseFd {
    #[inline(always)]
    fn into_raw_fd(self) -> RawFd {
        let fd = self.0;
        std::mem::forget(self);
        fd
    }
}

impl Drop for AutoCloseFd {
    fn drop(&mut self) {
        // SAFETY: Safe as long as we only store open file descriptors
        let rc = unsafe { ffi::coio_close(self.0) };
        if rc != 0 {
            crate::say_error!(
                "failed closing socket descriptor: {}",
                io::Error::last_os_error()
            );
        }
    }
}

/// Resolves provided url and port to a sequence of sock addrs.
///
/// # Returns
///
/// A vector of resolved addrs where v4 go first.
pub fn resolve_addr(url: &str, port: u16, timeout: f64) -> Result<Vec<SockAddr>, Error> {
    // SAFETY: value is not used inled hints are set
    let mut hints = unsafe { MaybeUninit::<libc::addrinfo>::zeroed().assume_init() };

    hints.ai_family = libc::AF_UNSPEC;
    hints.ai_socktype = libc::SOCK_STREAM;

    let host = CString::new(url).map_err(Error::ConstructCString)?;

    // SAFETY: safe as long as we are in tarantool runtime
    let addrinfo = match unsafe { crate::coio::getaddrinfo(&host, None, &hints, timeout) } {
        Ok(v) => v,
        Err(e) => {
            match e {
                crate::error::Error::IO(ref ee) => {
                    if let io::ErrorKind::TimedOut = ee.kind() {
                        return Err(Error::Timeout);
                    }
                }
                crate::error::Error::Tarantool(ref ee) => {
                    if let Some(ref kind) = ee.error_type {
                        let kind: &str = kind;
                        if kind == "TimedOut" {
                            return Err(Error::Timeout);
                        }
                    }
                }
                _ => (),
            }
            crate::say_error!("coio_getaddrinfo failed: {e}");
            return Err(Error::ResolveAddress(url.into()));
        }
    };

    let mut result = Vec::with_capacity(4);
    let mut current = addrinfo;

    while !current.is_null() {
        // SAFETY: we are dereferencing pointers which were allocated by libc so it's fine
        let ai = unsafe { *current };
        match ai.ai_family {
            libc::AF_INET => {
                // SAFETY: we are dereferencing pointers which were allocated by libc so it's fine
                let mut sockaddr = unsafe { *(ai.ai_addr as *mut libc::sockaddr_in) };
                sockaddr.sin_port = port.to_be();
                result.push(SockAddr::V4(sockaddr));
            }
            libc::AF_INET6 => {
                // SAFETY: we are dereferencing pointers which were allocated by libc so it's fine
                let mut sockaddr = unsafe { *(ai.ai_addr as *mut libc::sockaddr_in6) };
                sockaddr.sin6_port = port.to_be();
                result.push(SockAddr::V6(sockaddr));
            }
            af => {
                // SAFETY: value was allocated by libc so it's fine
                unsafe { libc::freeaddrinfo(addrinfo) };
                return Err(Error::UnknownAddressFamily(af as u16));
            }
        }
        current = ai.ai_next;
    }

    // SAFETY: value was allocated by libc so it's fine
    unsafe { libc::freeaddrinfo(addrinfo) };

    // Sort resolved addrs to prefer v4
    result.sort();

    Ok(result)
}

/// # Safety
/// addr_info.add should be a valid
pub unsafe fn connect_socket(addr_info: &AddrInfo<'_>) -> io::Result<AutoCloseFd> {
    let fd = nonblocking_socket(addr_info.kind)?;
    let Err(e) = cvt(libc::connect(
        fd.as_raw_fd(),
        addr_info.addr,
        addr_info.addr_len,
    )) else {
        return Ok(fd);
    };
    if e.raw_os_error() != Some(libc::EINPROGRESS) {
        return Err(e);
    }
    Ok(fd)
}

/// # Safety
/// addr_info.add should be a valid
pub unsafe fn bind_socket(addr_info: &AddrInfo<'_>) -> io::Result<AutoCloseFd> {
    let fd = nonblocking_socket(addr_info.kind)?;
    cvt(libc::bind(
        fd.as_raw_fd(),
        addr_info.addr,
        addr_info.addr_len,
    ))?;
    setsockopt(
        fd.as_raw_fd(),
        libc::SOL_SOCKET,
        libc::SO_REUSEADDR,
        1 as c_int,
    )?;
    Ok(fd)
}

pub fn setsockopt<T>(
    sock: RawFd,
    level: c_int,
    option_name: c_int,
    option_value: T,
) -> io::Result<()> {
    unsafe {
        cvt(libc::setsockopt(
            sock,
            level,
            option_name,
            (&raw const option_value) as *const _,
            size_of::<T>() as libc::socklen_t,
        ))?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
#[inline(always)]
pub fn nonblocking_socket(kind: libc::c_int) -> io::Result<AutoCloseFd> {
    // SAFETY: This is safe because `libc::socket` doesn't do undefined behavior
    unsafe {
        let raw_fd = cvt(libc::socket(
            kind,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            0,
        ))?;
        let fd = AutoCloseFd::from_raw_fd(raw_fd);

        Ok(fd)
    }
}

#[cfg(target_os = "macos")]
pub fn nonblocking_socket(kind: libc::c_int) -> io::Result<AutoCloseFd> {
    // SAFETY: This is safe because `libc::socket` doesn't do undefined behavior
    let fd = unsafe { AutoCloseFd::from_raw_fd(cvt(libc::socket(kind, libc::SOCK_STREAM, 0))?) };
    // SAFETY: safe as fd is just openned.
    unsafe { make_socket_nonblocking(fd.as_raw_fd())? };
    Ok(fd)
}

/// SAFETY: safe as long as fd is currently open.
#[cfg(target_os = "macos")]
unsafe fn make_socket_nonblocking(fd: RawFd) -> io::Result<()> {
    // SAFETY: This is safe because fd is open
    cvt(libc::ioctl(fd, libc::FIOCLEX))?;
    let opt_value = 1;
    // SAFETY: This is safe because fd is open and the opt_value buffer specification is valid.
    cvt(libc::setsockopt(
        fd.as_raw_fd(),
        libc::SOL_SOCKET,
        libc::SO_NOSIGPIPE,
        &opt_value as *const _ as *const libc::c_void,
        mem::size_of_val(&opt_value) as _,
    ))?;
    // SAFETY: This is safe because fd is open
    cvt(libc::ioctl(fd.as_raw_fd(), libc::FIONBIO, &mut 1))?;
    Ok(())
}

#[cfg(target_os = "linux")]
#[inline(always)]
pub fn accept(fd: RawFd) -> io::Result<AutoCloseFd> {
    let mut dummy = std::mem::MaybeUninit::<libc::sockaddr>::uninit();
    let mut dummy_size = std::mem::size_of_val(&dummy) as _;
    // SAFETY: This is safe because `libc::accept4` doesn't do undefined behavior
    return unsafe {
        Ok(AutoCloseFd::from_raw_fd(cvt(libc::accept4(
            fd,
            dummy.as_mut_ptr(),
            &mut dummy_size,
            libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
        ))?))
    };
}

#[cfg(target_os = "macos")]
#[inline(always)]
pub fn accept(fd: RawFd) -> io::Result<AutoCloseFd> {
    let mut dummy = std::mem::MaybeUninit::<libc::sockaddr>::uninit();
    let mut dummy_size = std::mem::size_of_val(&dummy) as _;
    // SAFETY: This is safe because `libc::accept` doesn't do undefined behavior
    unsafe {
        let fd =
            AutoCloseFd::from_raw_fd(cvt(libc::accept(fd, dummy.as_mut_ptr(), &mut dummy_size))?);
        make_socket_nonblocking(fd.as_raw_fd())?;
        Ok(fd)
    }
}

pub fn check_socket_error(fd: &impl AsRawFd) -> io::Result<()> {
    // SAFETY: passed only to ffi call so it's fine
    let mut val: libc::c_int = 0;
    let mut val_len = mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: fd is not closed since it is inside OwnedFd
    cvt(unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            &mut val as *mut libc::c_int as *mut _,
            &mut val_len,
        )
    })?;
    match val {
        0 => Ok(()),
        v => Err(io::Error::from_raw_os_error(v as i32)),
    }
}

#[derive(Debug)]
pub enum SockAddr {
    V4(libc::sockaddr_in),
    V6(libc::sockaddr_in6),
}

impl Ord for SockAddr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (SockAddr::V4(_), SockAddr::V6(_)) => std::cmp::Ordering::Less,
            (SockAddr::V6(_), SockAddr::V4(_)) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for SockAddr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SockAddr {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (SockAddr::V4(_), SockAddr::V4(_)) | (SockAddr::V6(_), SockAddr::V6(_))
        )
    }
}

impl Eq for SockAddr {}

pub struct AddrInfo<'a> {
    kind: libc::c_int,
    addr: *const libc::sockaddr,
    addr_len: libc::socklen_t,
    marker: marker::PhantomData<&'a ()>,
}

impl<'a> From<&'a SockAddr> for AddrInfo<'a> {
    fn from(value: &'a SockAddr) -> Self {
        let (kind, addr, addr_len) = match value {
            SockAddr::V4(v4) => {
                let kind = libc::AF_INET;
                let addr = v4 as *const libc::sockaddr_in as *const libc::sockaddr;
                let addr_len = mem::size_of::<libc::sockaddr_in>();
                (kind, addr, addr_len)
            }
            SockAddr::V6(v6) => {
                let kind = libc::AF_INET6;
                let addr = v6 as *const libc::sockaddr_in6 as *const libc::sockaddr;
                let addr_len = mem::size_of::<libc::sockaddr_in6>();
                (kind, addr, addr_len)
            }
        };
        Self {
            kind,
            addr,
            addr_len: addr_len as _,
            marker: marker::PhantomData::<&'a ()>,
        }
    }
}
