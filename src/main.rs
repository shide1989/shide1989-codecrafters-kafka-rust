#![allow(unused_imports)]
use bytes::{Buf, BufMut, Bytes, BytesMut};
use codecrafters_kafka::messaging::*;
use codecrafters_kafka::versions::*;

use std::io::prelude::*;

use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};
const MESSAGE_SIZE_BYTES: usize = 4;
// Request message size header
fn handle_connection(mut stream: TcpStream) {
    println!("accepted new connection");
    // stream
    //     .set_nonblocking(true)
    //     .expect("set_nonblocking call failed");

    let mut buf: BytesMut = BytesMut::zeroed(MESSAGE_SIZE_BYTES);
    // Read the message size header
    while let Ok(len) = stream.peek(&mut buf) {
        if len == 0 {
            println!("No data left to read.");
            break;
        }
        let bytes: usize = buf.get_i32() as usize;
        buf = BytesMut::zeroed(MESSAGE_SIZE_BYTES); // reset
        println!("Bytes read {len}: {bytes}");
        let mut buf: BytesMut = BytesMut::zeroed(bytes + MESSAGE_SIZE_BYTES);
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
        stream
            .write_all(&buf)
            .expect("Stream should have been written.");
        stream.flush().unwrap();
        println!("Stream written.");
    }
    stream
        .shutdown(std::net::Shutdown::Both)
        .expect("Stream should be connected");
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092")?;

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
