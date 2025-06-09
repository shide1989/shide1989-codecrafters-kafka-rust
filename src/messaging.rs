use crate::versions::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct Response {
    // Response header
    pub correlation_id: i32,
    // Response body
    pub error_code: i16,
    pub api_versions: SupportedVersions,
}

// const RESPONSE_CAPACITY: usize = 32;
const HEADER_SIZE: i32 = 4;
const ERROR_CODE_SIZE: i32 = 2;
const THROTTLE_TIME_MS_SIZE: i32 = 4;
const TAG_BUFFER_SIZE: i32 = 1;

const THROTTLE_TIMER: i32 = 0;
const TAG_BUFFER: i8 = 0;

impl Response {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Message size on 4bytes
        buf.put_i32(self.size());

        // Response Header on 4 bytes
        buf.put_i32(self.header());

        // Response body
        // Error code on 2 bytes
        buf.put_i16(self.error_code);

        // API versions array on 1 + 7 * n bytes
        // https://binspec.org/kafka-api-versions-Response-v4?highlight=10-31
        buf.extend_from_slice(&Into::<Bytes>::into(self.api_versions));

        buf.put_i32(THROTTLE_TIMER);
        buf.put_i8(TAG_BUFFER);

        // Convert to Bytes (zero-copy)
        buf.freeze()
    }

    fn size(&self) -> i32 {
        API_VERSIONS.size() as i32
            + HEADER_SIZE
            + ERROR_CODE_SIZE
            + THROTTLE_TIME_MS_SIZE
            + TAG_BUFFER_SIZE
    }

    fn header(&self) -> i32 {
        self.correlation_id
    }
}

#[derive(Debug)]
#[allow(unused)]
pub struct Request {
    message_size: i32,
    request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
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
