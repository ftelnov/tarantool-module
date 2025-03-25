use std::{ffi::NulError, io};

pub mod listener;
pub mod stream;
mod utils;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("failed to resolve domain name '{0}'")]
    ResolveAddress(String),
    #[error("input parameters contain ffi incompatible strings: {0}")]
    ConstructCString(NulError),
    #[error("failed to connect to address '{address}': {error}")]
    Connect { error: io::Error, address: String },
    #[error("failed to bind to {address}: {error}")]
    Bind { error: io::Error, address: String },
    #[error("failed to accept connection on socket")]
    Accept { error: io::Error },
    #[error("unknown address family: {0}")]
    UnknownAddressFamily(u16),
    #[error("write half of the stream is closed")]
    WriteClosed,
    #[error("connect timeout")]
    Timeout,
    #[error("socket is already closed")]
    AlreadyClosed,
}
