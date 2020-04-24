// Copyright (c) 2019 Ant Financial
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use nix::sys::socket::*;
use std::os::unix::io::RawFd;

use crate::error::{get_rpc_status, Error, Result};
use crate::ttrpc::Code;

/// ProtoBuf request message
pub const MESSAGE_TYPE_REQUEST: u8 = 0x1;
/// ProtoBuf reply message
pub const MESSAGE_TYPE_RESPONSE: u8 = 0x2;

const MESSAGE_HEADER_LENGTH: usize = 10;
const MESSAGE_LENGTH_MAX: usize = 4 << 20;

const SOCK_DISCONNECTED: &str = "socket disconnected";

fn sock_error_msg(closed: bool, msg: String) -> Error {
    if closed {
        return Error::Socket(SOCK_DISCONNECTED.to_string());
    }

    get_rpc_status(Code::INVALID_ARGUMENT, msg)
}

/// ProtoBuf message header.
#[derive(Default, Debug)]
pub struct MessageHeader {
    pub length: u32,
    pub stream_id: u32,
    pub type_: u8,
    pub flags: u8,
}

impl MessageHeader {
    /// Decode a message header from a data buffer encoded by the ProtoBuf protocol.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() != MESSAGE_HEADER_LENGTH {
            return Err(sock_error_msg(
                false,
                format!("Message header length {} is incorrect", buf.len()),
            ));
        }

        let mut mh = MessageHeader::default();
        let mut covbuf: &[u8] = &buf[..4];
        mh.length = covbuf.read_u32::<BigEndian>().map_err(err_to_RpcStatus!(
            Code::INVALID_ARGUMENT,
            e,
            ""
        ))?;
        let mut covbuf: &[u8] = &buf[4..8];
        mh.stream_id = covbuf.read_u32::<BigEndian>().map_err(err_to_RpcStatus!(
            Code::INVALID_ARGUMENT,
            e,
            ""
        ))?;
        mh.type_ = buf[8];
        mh.flags = buf[9];

        if !mh.is_valid() {
            return Err(get_rpc_status(
                Code::INVALID_ARGUMENT,
                format!(
                    "message length {} exceed maximum message size of {}",
                    mh.length, MESSAGE_LENGTH_MAX
                ),
            ));
        }

        Ok(mh)
    }

    /// Encode a message header into a data buffer using the ProtoBuf protocol.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; MESSAGE_HEADER_LENGTH];

        let mut covbuf: &mut [u8] = &mut buf[..4];
        BigEndian::write_u32(&mut covbuf, self.length);
        let mut covbuf: &mut [u8] = &mut buf[4..8];
        BigEndian::write_u32(&mut covbuf, self.stream_id);
        buf[8] = self.type_;
        buf[9] = self.flags;

        buf
    }

    /// Check whether it's a request message.
    pub fn is_request(&self) -> bool {
        self.type_ == MESSAGE_TYPE_REQUEST
    }

    /// Check whether it's a response message.
    pub fn is_response(&self) -> bool {
        self.type_ == MESSAGE_TYPE_RESPONSE
    }

    /// Check whether the message header is valid or not.
    pub fn is_valid(&self) -> bool {
        self.length <= MESSAGE_LENGTH_MAX as u32 && (self.is_request() || self.is_response())
    }
}

/// A buffer to receive data from underlying socket.
pub struct ReadBuffer {
    buf: Vec<u8>,
    len: usize,
    pos: usize,
}

impl ReadBuffer {
    /// Create a ReadBuffer with capacity to receive `count` bytes.
    pub fn new(count: usize) -> Self {
        let mut buf: Vec<u8> = Vec::with_capacity(count);
        // It's safe because the vector is used as a buffer to receive message.
        unsafe {
            buf.set_len(count);
        }

        ReadBuffer {
            buf,
            len: count,
            pos: 0,
        }
    }

    /// Receive data from the socket `fd`.
    ///
    /// Returns:
    /// - Ok(n) when n > 0: bytes of data received.
    /// - Ok(0): socket has been closed.
    /// - Err(e): failed to receive data from the socket.
    pub fn recv(&mut self, fd: RawFd) -> Result<usize> {
        loop {
            match recv(fd, &mut self.buf[self.pos..], MsgFlags::empty()) {
                Ok(l) => {
                    self.pos += l;
                    return Ok(l);
                }

                Err(e) => {
                    if e != ::nix::Error::from_errno(::nix::errno::Errno::EINTR) {
                        return Err(Error::Socket(e.to_string()));
                    }
                }
            }
        }
    }

    /// Check whether all data has been received.
    pub fn is_ready(&self) -> bool {
        self.len == self.pos
    }
}

impl Into<Vec<u8>> for ReadBuffer {
    fn into(self) -> Vec<u8> {
        self.buf
    }
}

impl From<Vec<u8>> for ReadBuffer {
    fn from(buf: Vec<u8>) -> Self {
        let len = buf.len();

        ReadBuffer { buf, len, pos: 0 }
    }
}

fn read_count(fd: RawFd, count: usize) -> Result<Vec<u8>> {
    let mut buf = ReadBuffer::new(count);

    while !buf.is_ready() {
        match buf.recv(fd)? {
            0 => {
                return Err(sock_error_msg(
                    true,
                    format!(
                        "Socket get closed after receiving {} out of {} bytes data",
                        buf.pos, buf.len
                    ),
                ))
            }
            _ => continue,
        }
    }

    Ok(buf.into())
}

/// Receive a fully message from the underlying socket 'fd' in blocking mode.
pub fn read_message(fd: RawFd) -> Result<(MessageHeader, Vec<u8>)> {
    let buf = read_count(fd, MESSAGE_HEADER_LENGTH)?;
    let mh = MessageHeader::decode(&buf)?;
    trace!("Got Message header {:?}", mh);

    let buf = read_count(fd, mh.length as usize)?;
    trace!("Got Message body {:?}", buf);

    Ok((mh, buf))
}

/// A buffer to send data from underlying socket.
pub struct WriteBuffer {
    buf: Vec<u8>,
    len: usize,
    pos: usize,
}

impl WriteBuffer {
    /// Send data to the socket `fd`.
    ///
    /// Returns:
    /// - Ok(n) when n > 0: bytes of data sent.
    /// - Ok(0): socket has been closed.
    /// - Err(e): failed to send data from the socket.
    pub fn send(&mut self, fd: RawFd) -> Result<usize> {
        loop {
            match send(fd, &self.buf[self.pos..], MsgFlags::empty()) {
                Ok(l) => {
                    self.pos += l;
                    return Ok(l);
                }

                Err(e) => {
                    if e != ::nix::Error::from_errno(::nix::errno::Errno::EINTR) {
                        return Err(Error::Socket(e.to_string()));
                    }
                }
            }
        }
    }

    /// Check whether all data has been sent out.
    pub fn is_done(&self) -> bool {
        self.pos == self.len
    }
}

impl Into<Vec<u8>> for WriteBuffer {
    fn into(self) -> Vec<u8> {
        self.buf
    }
}

impl From<Vec<u8>> for WriteBuffer {
    fn from(buf: Vec<u8>) -> Self {
        let len = buf.len();

        WriteBuffer { buf, len, pos: 0 }
    }
}

fn write_count(fd: RawFd, buf: &mut WriteBuffer) -> Result<()> {
    while !buf.is_done() {
        match buf.send(fd)? {
            0 => {
                return Err(sock_error_msg(
                    true,
                    format!(
                        "Socket gets closed after sending {} out of {} bytes data",
                        buf.pos, buf.len
                    ),
                ))
            }
            _ => continue,
        }
    }

    Ok(())
}

/// Send a fully message to the underlying socket 'fd' in blocking mode.
pub fn write_message(fd: RawFd, mh: MessageHeader, buf: Vec<u8>) -> Result<()> {
    let mut hbuf = WriteBuffer::from(mh.encode());
    write_count(fd, &mut hbuf)?;

    let mut wbuf = WriteBuffer::from(buf);
    write_count(fd, &mut wbuf)
}
