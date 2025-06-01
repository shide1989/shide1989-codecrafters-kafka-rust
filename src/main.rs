#![allow(unused_imports)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::prelude::*;

use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

struct Response {
    message_size: i32,
    correlation_id: i32,
    error_code: i16,
}

impl Response {
    fn to_buffer(&self) -> [u8; 10] {
        let mut buf = [0; 10];
        buf[0..4].copy_from_slice(&self.message_size.to_be_bytes());
        buf[4..8].copy_from_slice(&self.correlation_id.to_be_bytes());
        buf[8..10].copy_from_slice(&self.error_code.to_be_bytes());
        buf
    }
}

#[derive(Debug)]
struct Request {
    message_size: i32,
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
}

impl TryFrom<Bytes> for Request {
    type Error = &'static str;

    fn try_from(mut value: Bytes) -> Result<Self, Self::Error> {
        if value.len() == 0 {
            Err("BytesMut value is empty")
        } else {
            Ok(Request {
                message_size: value.get_i32(),
                request_api_key: value.get_i16(),
                request_api_version: value.get_i16(),
                correlation_id: value.get_i32(),
            })
        }
    }
}

impl Into<Bytes> for Request {
    fn into(self) -> Bytes {
        let mut buf = BytesMut::new();

        buf.put_i32(self.message_size);
        buf.put_i32(self.correlation_id);
        buf.put_i16(self.request_api_key);
        buf.put_i16(self.request_api_version);

        buf.freeze()
    }
}

fn handle_connection(mut stream: TcpStream) {
    println!("accepted new connection");
    // For now assume we read only the first 12 bytes.
    let capacity: usize = 12;

    let mut buf = BytesMut::zeroed(capacity);

    if let Err(e) = stream.read(&mut buf) {
        eprintln!("Error reading from stream: {e}");
        return;
    }
    let req = Request::try_from(buf.freeze()).expect("Request should have been parsed.");

    println!("Parsed request: {req:?}");

    let res = Response {
        correlation_id: req.correlation_id,
        message_size: 0,
        error_code: 35,
    };
    let buf = res.to_buffer();

    stream.write_all(&buf).unwrap();
    stream.flush().unwrap();
    stream.shutdown(std::net::Shutdown::Both).unwrap();

    println!("Stream written.")
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_connection(stream);
            }
            Err(e) => {
                eprintln!("Connection failed {e}")
            }
        }
    }
    Ok(())
}
