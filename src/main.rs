#![allow(unused_imports)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use codecrafters_kafka::messaging::*;
use codecrafters_kafka::versions::*;

use std::io::prelude::*;

use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

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

    let error_code = if req.request_api_version == 4 {
        0
    } else {
        UNSUPPORTED_VERSION_CODE
    };

    let res = Response {
        correlation_id: req.correlation_id,
        error_code,
        api_versions: API_VERSIONS,
    };
    let buf = res.to_bytes();

    println!("Sending response {res:?}");
    println!("Sending buffer {buf:?}");
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
