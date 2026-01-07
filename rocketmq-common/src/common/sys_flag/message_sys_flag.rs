use crate::common::compression::compression_type::CompressionType;

// Meaning of each bit in the system flag
///
/// | bit    | 7 | 6 | 5         | 4        | 3           | 2                | 1                | 0                |
/// |--------|---|---|-----------|----------|-------------|------------------|------------------|------------------|
/// | byte 1 |   |   | STOREHOST | BORNHOST | TRANSACTION | TRANSACTION      | MULTI_TAGS       | COMPRESSED       |
/// | byte 2 |   |   |           |          |             | COMPRESSION_TYPE | COMPRESSION_TYPE | COMPRESSION_TYPE |
/// | byte 3 |   |   |           |          |             |                  |                  |                  |
/// | byte 4 |   |   |           |          |             |                  |                  |
pub struct MessageSysFlag;

impl MessageSysFlag {
    pub const COMPRESSED_FLAG: i32 = 0x1;
    pub const MULTI_TAGS_FLAG: i32 = 0x1 << 1;

    //Transaction related flag
    pub const TRANSACTION_NOT_TYPE: i32 = 0;
    pub const TRANSACTION_PREPARED_TYPE: i32 = 0x1 << 2;
    pub const TRANSACTION_COMMIT_TYPE: i32 = 0x2 << 2;
    pub const TRANSACTION_ROLLBACK_TYPE: i32 = 0x3 << 2;

    //Flag of the message properties
    pub const BORNHOST_V6_FLAG: i32 = 0x1 << 4;
    pub const STOREHOSTADDRESS_V6_FLAG: i32 = 0x1 << 5;

    //Mark the flag for batch to avoid conflict
    pub const NEED_UNWRAP_FLAG: i32 = 0x1 << 6;
    pub const INNER_BATCH_FLAG: i32 = 0x1 << 7;

    // COMPRESSION_TYPE
    pub const COMPRESSION_LZ4_TYPE: i32 = 0x1 << 8;
    pub const COMPRESSION_ZSTD_TYPE: i32 = 0x2 << 8;
    pub const COMPRESSION_ZLIB_TYPE: i32 = 0x3 << 8;
    pub const COMPRESSION_TYPE_COMPARATOR: i32 = 0x7 << 8;

    #[inline]
    pub const fn get_transaction_value(flag: i32) -> i32 {
        flag & Self::TRANSACTION_ROLLBACK_TYPE
    }

    #[inline]
    pub const fn reset_transaction_value(flag: i32, transaction_type: i32) -> i32 {
        (flag & !Self::TRANSACTION_ROLLBACK_TYPE) | transaction_type
    }

    #[inline]
    pub const fn clear_compressed_flag(flag: i32) -> i32 {
        flag & !Self::COMPRESSED_FLAG
    }

    #[inline]
    pub fn get_compression_type(flag: i32) -> CompressionType {
        let compression_type_value = (flag & Self::COMPRESSION_TYPE_COMPARATOR) >> 8;
        CompressionType::find_by_value(compression_type_value)
    }

    #[inline]
    pub const fn check(flag: i32, expected_flag: i32) -> bool {
        (flag & expected_flag) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_transaction_value_returns_correct_value() {
        let flag = MessageSysFlag::TRANSACTION_COMMIT_TYPE;
        assert_eq!(
            MessageSysFlag::get_transaction_value(flag),
            MessageSysFlag::TRANSACTION_COMMIT_TYPE
        );
    }

    #[test]
    fn reset_transaction_value_resets_correctly() {
        let flag = MessageSysFlag::TRANSACTION_COMMIT_TYPE;
        let new_flag = MessageSysFlag::reset_transaction_value(flag, MessageSysFlag::TRANSACTION_ROLLBACK_TYPE);
        assert_eq!(new_flag, MessageSysFlag::TRANSACTION_ROLLBACK_TYPE);
    }

    #[test]
    fn clear_compressed_flag_clears_correctly() {
        let flag = MessageSysFlag::COMPRESSED_FLAG;
        let new_flag = MessageSysFlag::clear_compressed_flag(flag);
        assert_eq!(new_flag, 0);
    }

    #[test]
    fn get_compression_type_returns_correct_type() {
        let flag = MessageSysFlag::COMPRESSION_LZ4_TYPE;
        assert_eq!(MessageSysFlag::get_compression_type(flag), CompressionType::LZ4);
    }

    #[test]
    fn check_returns_true_when_flag_is_set() {
        let flag = MessageSysFlag::COMPRESSED_FLAG;
        assert!(MessageSysFlag::check(flag, MessageSysFlag::COMPRESSED_FLAG));
    }

    #[test]
    fn check_returns_false_when_flag_is_not_set() {
        let flag = 0;
        assert!(!MessageSysFlag::check(flag, MessageSysFlag::COMPRESSED_FLAG));
    }
}
