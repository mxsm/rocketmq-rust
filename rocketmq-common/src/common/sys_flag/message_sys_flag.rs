use crate::common::compression::compression_type::CompressionType;

pub struct MessageSysFlag;

impl MessageSysFlag {
    pub const COMPRESSED_FLAG: i32 = 0x1;
    pub const MULTI_TAGS_FLAG: i32 = 0x1 << 1;
    pub const TRANSACTION_NOT_TYPE: i32 = 0;
    pub const TRANSACTION_PREPARED_TYPE: i32 = 0x1 << 2;
    pub const TRANSACTION_COMMIT_TYPE: i32 = 0x2 << 2;
    pub const TRANSACTION_ROLLBACK_TYPE: i32 = 0x3 << 2;
    pub const BORNHOST_V6_FLAG: i32 = 0x1 << 4;
    pub const STOREHOSTADDRESS_V6_FLAG: i32 = 0x1 << 5;
    pub const NEED_UNWRAP_FLAG: i32 = 0x1 << 6;
    pub const INNER_BATCH_FLAG: i32 = 0x1 << 7;

    // COMPRESSION_TYPE
    pub const COMPRESSION_LZ4_TYPE: i32 = 0x1 << 8;
    pub const COMPRESSION_ZSTD_TYPE: i32 = 0x2 << 8;
    pub const COMPRESSION_ZLIB_TYPE: i32 = 0x3 << 8;
    pub const COMPRESSION_TYPE_COMPARATOR: i32 = 0x7 << 8;

    pub fn get_transaction_value(flag: i32) -> i32 {
        flag & Self::TRANSACTION_ROLLBACK_TYPE
    }

    pub fn reset_transaction_value(flag: i32, transaction_type: i32) -> i32 {
        (flag & !Self::TRANSACTION_ROLLBACK_TYPE) | transaction_type
    }

    pub fn clear_compressed_flag(flag: i32) -> i32 {
        flag & !Self::COMPRESSED_FLAG
    }

    pub fn get_compression_type(flag: i32) -> CompressionType {
        let compression_type_value = (flag & Self::COMPRESSION_TYPE_COMPARATOR) >> 8;
        CompressionType::find_by_value(compression_type_value)
    }

    pub fn check(flag: i32, expected_flag: i32) -> bool {
        (flag & expected_flag) != 0
    }
}
