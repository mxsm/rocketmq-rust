use crate::common::sys_flag::message_sys_flag::MessageSysFlag;

#[derive(Debug, PartialEq)]
pub enum CompressionType {
    LZ4,
    Zstd,
    Zlib,
}

impl CompressionType {
    pub fn of(name: &str) -> Self {
        match name.trim().to_uppercase().as_str() {
            "LZ4" => Self::LZ4,
            "ZSTD" => Self::Zstd,
            "ZLIB" => Self::Zlib,
            _ => panic!("Unsupported compress type name: {}", name),
        }
    }

    pub fn find_by_value(value: i32) -> Self {
        match value {
            1 => Self::LZ4,
            2 => Self::Zstd,
            0 | 3 => Self::Zlib,
            _ => panic!("Unknown compress type value: {}", value),
        }
    }

    pub fn get_compression_flag(&self) -> i32 {
        match self {
            Self::LZ4 => MessageSysFlag::COMPRESSION_LZ4_TYPE,
            Self::Zstd => MessageSysFlag::COMPRESSION_ZSTD_TYPE,
            Self::Zlib => MessageSysFlag::COMPRESSION_ZLIB_TYPE,
        }
    }
}
