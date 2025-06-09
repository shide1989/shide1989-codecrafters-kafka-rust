use bytes::{BufMut, Bytes, BytesMut};
pub const UNSUPPORTED_VERSION_CODE: i16 = 35;

pub const API_VERSIONS: SupportedApiKeys = SupportedApiKeys {
    versions: [
        ApiKey {
            api_key: 18, // APIVersions
            min_version: 4,
            max_version: 4,
            tag_buffer: 0,
        },
        ApiKey {
            api_key: 75, // DescribeTopicPartitions
            min_version: 0,
            max_version: 0,
            tag_buffer: 0,
        },
    ],
};

#[derive(Clone, Copy, Debug)]
pub struct ApiKey {
    api_key: i16,
    min_version: i16,
    max_version: i16,
    tag_buffer: i8,
}

impl Into<Bytes> for ApiKey {
    fn into(self) -> Bytes {
        let mut src = BytesMut::with_capacity(7);
        src.put_i16(self.api_key);
        src.put_i16(self.min_version);
        src.put_i16(self.max_version);
        src.put_i8(self.tag_buffer);

        src.freeze()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SupportedApiKeys {
    versions: [ApiKey; 2],
}

impl SupportedApiKeys {
    pub fn size(&self) -> usize {
        println!("versions: {:?}", self.versions.len());
        1 + 7 * self.versions.len() // 1 for the array length
    }
}

impl Into<Bytes> for SupportedApiKeys {
    fn into(self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.size());
        // The length of the API Versions array + 1, encoded as a varint.
        // Here, it is 0x02 (2), meaning that the array length is 1.
        buf.put_i8(self.versions.len() as i8 + 1);

        for version in self.versions {
            buf.extend_from_slice(&Into::<Bytes>::into(version));
        }
        buf.freeze()
    }
}
