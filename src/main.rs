#![allow(unused_imports)]
use bytes::{BufMut, Bytes, BytesMut};
use std::{io::Write, net::TcpListener};

struct Response {
    message_size: i32,
    correlation_id: i32,
}

impl Response {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8); // 4 bytes for each i32
        buf.put_i32(self.message_size);
        buf.put_i32(self.correlation_id);
        buf.freeze()
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let res = Response {
                    correlation_id: 7,
                    message_size: 0,
                };
                let bytes = res.to_bytes();

                if let Err(e) = _stream.write_all(&bytes) {
                    eprintln!("Error writing to stream: {e}");
                };
                println!("Stream written.")
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
