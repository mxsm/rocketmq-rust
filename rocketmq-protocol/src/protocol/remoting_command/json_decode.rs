// Copyright 2026 The RocketMQ Rust Authors
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

use std::ops::Range;

use bytes::Bytes;
use bytes::BytesMut;
use cheetah_string::CheetahString;

use super::extension_fields::ExtensionFields;
use super::RemotingCommand;
use crate::protocol::header_codec::JsonHeaderFields;
use crate::protocol::LanguageCode;
use crate::protocol::SerializeType;

const CODE: u8 = 1 << 0;
const LANGUAGE: u8 = 1 << 1;
const VERSION: u8 = 1 << 2;
const OPAQUE: u8 = 1 << 3;
const FLAG: u8 = 1 << 4;
const REMARK: u8 = 1 << 5;
const EXT_FIELDS: u8 = 1 << 6;
const SERIALIZE_TYPE: u8 = 1 << 7;
const REQUIRED_FIELDS: u8 = CODE | LANGUAGE | VERSION | OPAQUE | FLAG | SERIALIZE_TYPE;

struct JsonHeaderParser<'a> {
    src: &'a [u8],
    cursor: usize,
}

impl<'a> JsonHeaderParser<'a> {
    #[inline]
    fn new(src: &'a [u8]) -> Self {
        Self { src, cursor: 0 }
    }

    #[inline]
    fn skip_whitespace(&mut self) {
        while matches!(self.src.get(self.cursor), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.cursor += 1;
        }
    }

    #[inline]
    fn consume(&mut self, expected: u8) -> Option<()> {
        self.skip_whitespace();
        if self.src.get(self.cursor).copied()? != expected {
            return None;
        }
        self.cursor += 1;
        Some(())
    }

    #[inline]
    fn consume_null(&mut self) -> Option<()> {
        self.skip_whitespace();
        if !self.src.get(self.cursor..)?.starts_with(b"null") {
            return None;
        }
        self.cursor += 4;
        Some(())
    }

    #[inline]
    fn consume_literal(&mut self, expected: &[u8]) -> Option<()> {
        if !self.src.get(self.cursor..)?.starts_with(expected) {
            return None;
        }
        self.cursor += expected.len();
        Some(())
    }

    #[inline]
    fn parse_string(&mut self) -> Option<Range<usize>> {
        self.consume(b'"')?;
        let start = self.cursor;
        loop {
            match self.src.get(self.cursor).copied()? {
                b'"' => {
                    let end = self.cursor;
                    self.cursor += 1;
                    return Some(start..end);
                }
                b'\\' | 0..=0x1f => return None,
                _ => self.cursor += 1,
            }
        }
    }

    #[inline]
    fn parse_canonical_string(&mut self) -> Option<Range<usize>> {
        if self.src.get(self.cursor).copied()? != b'"' {
            return None;
        }
        self.cursor += 1;
        let start = self.cursor;
        loop {
            match self.src.get(self.cursor).copied()? {
                b'"' => {
                    let end = self.cursor;
                    self.cursor += 1;
                    return Some(start..end);
                }
                b'\\' | 0..=0x1f => return None,
                _ => self.cursor += 1,
            }
        }
    }

    #[inline]
    fn parse_canonical_json_string(&mut self) -> Option<bool> {
        if self.src.get(self.cursor).copied()? != b'"' {
            return None;
        }
        self.cursor += 1;
        let start = self.cursor;
        let mut escaped = false;
        let mut string_bytes = 0u8;
        loop {
            match self.src.get(self.cursor).copied()? {
                b'"' => {
                    if string_bytes & 0x80 != 0 {
                        std::str::from_utf8(&self.src[start..self.cursor]).ok()?;
                    }
                    self.cursor += 1;
                    return Some(escaped);
                }
                b'\\' => {
                    escaped = true;
                    self.cursor += 1;
                    match self.src.get(self.cursor).copied()? {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => self.cursor += 1,
                        b'u' => {
                            let digits = self.src.get(self.cursor + 1..self.cursor + 5)?;
                            if !digits.iter().all(u8::is_ascii_hexdigit) {
                                return None;
                            }
                            self.cursor += 5;
                        }
                        _ => return None,
                    }
                }
                0..=0x1f => return None,
                byte => {
                    string_bytes |= byte;
                    self.cursor += 1;
                }
            }
        }
    }

    #[inline]
    fn parse_i32(&mut self) -> Option<i32> {
        self.skip_whitespace();
        let negative = if self.src.get(self.cursor) == Some(&b'-') {
            self.cursor += 1;
            true
        } else {
            false
        };
        let digits_start = self.cursor;
        match self.src.get(self.cursor).copied()? {
            b'0' => {
                self.cursor += 1;
                if matches!(self.src.get(self.cursor), Some(b'0'..=b'9')) {
                    return None;
                }
            }
            b'1'..=b'9' => {
                self.cursor += 1;
                while matches!(self.src.get(self.cursor), Some(b'0'..=b'9')) {
                    self.cursor += 1;
                }
            }
            _ => return None,
        }
        let value = self
            .src
            .get(digits_start..self.cursor)?
            .iter()
            .try_fold(0i32, |value, byte| {
                value.checked_mul(10)?.checked_sub(i32::from(byte - b'0'))
            })?;
        if negative {
            Some(value)
        } else {
            value.checked_neg()
        }
    }

    fn parse_extension_fields(&mut self) -> Option<(Range<usize>, usize)> {
        self.consume(b'{')?;
        let start = self.cursor;
        let mut entry_count = 0usize;
        self.skip_whitespace();
        if self.src.get(self.cursor) == Some(&b'}') {
            let end = self.cursor;
            self.cursor += 1;
            return Some((start..end, 0));
        }

        loop {
            self.parse_string()?;
            self.consume(b':')?;
            self.parse_string()?;
            entry_count = entry_count.checked_add(1)?;
            self.skip_whitespace();
            match self.src.get(self.cursor).copied()? {
                b',' => self.cursor += 1,
                b'}' => {
                    let end = self.cursor;
                    self.cursor += 1;
                    return Some((start..end, entry_count));
                }
                _ => return None,
            }
        }
    }

    fn parse_canonical_extension_fields(&mut self) -> Option<ParsedExtensionFields> {
        if self.src.get(self.cursor).copied()? != b'{' {
            return None;
        }
        self.cursor += 1;
        let start = self.cursor;
        let mut entry_count = 0usize;
        if self.src.get(self.cursor) == Some(&b'}') {
            let end = self.cursor;
            self.cursor += 1;
            return Some(ParsedExtensionFields::Borrowed {
                range: start..end,
                entry_count: 0,
                canonical: true,
            });
        }

        loop {
            let key_escaped = self.parse_canonical_json_string()?;
            if self.src.get(self.cursor).copied()? != b':' {
                return None;
            }
            self.cursor += 1;
            let value_escaped = self.parse_canonical_json_string()?;
            entry_count = entry_count.checked_add(1)?;
            if key_escaped || value_escaped {
                return self.finish_length_prefixed_extension_fields(start, entry_count);
            }
            match self.src.get(self.cursor).copied()? {
                b',' => self.cursor += 1,
                b'}' => {
                    let end = self.cursor;
                    self.cursor += 1;
                    return Some(ParsedExtensionFields::Borrowed {
                        range: start..end,
                        entry_count,
                        canonical: true,
                    });
                }
                _ => return None,
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn finish_length_prefixed_extension_fields(
        &mut self,
        start: usize,
        mut entry_count: usize,
    ) -> Option<ParsedExtensionFields> {
        let capacity = self.src.len().checked_sub(start)?;
        let mut payload = decode_length_prefixed_fields(&self.src[start..self.cursor], entry_count, capacity)?;
        loop {
            match self.src.get(self.cursor).copied()? {
                b'}' => {
                    self.cursor += 1;
                    return Some(ParsedExtensionFields::LengthPrefixed { payload, entry_count });
                }
                b',' => self.cursor += 1,
                _ => return None,
            }
            append_json_string(self.src, &mut self.cursor, &mut payload)?;
            if self.src.get(self.cursor).copied()? != b':' {
                return None;
            }
            self.cursor += 1;
            append_json_string(self.src, &mut self.cursor, &mut payload)?;
            entry_count = entry_count.checked_add(1)?;
        }
    }
}

fn decode_hex_u16(src: &[u8]) -> Option<u16> {
    if src.len() != 4 {
        return None;
    }
    src.iter().try_fold(0u16, |value, byte| {
        let digit = match byte {
            b'0'..=b'9' => u16::from(byte - b'0'),
            b'a'..=b'f' => u16::from(byte - b'a' + 10),
            b'A'..=b'F' => u16::from(byte - b'A' + 10),
            _ => return None,
        };
        value.checked_mul(16)?.checked_add(digit)
    })
}

fn append_json_string(src: &[u8], cursor: &mut usize, out: &mut Vec<u8>) -> Option<()> {
    if src.get(*cursor).copied()? != b'"' {
        return None;
    }
    *cursor += 1;
    let length_offset = out.len();
    out.extend_from_slice(&[0; 4]);
    let value_offset = out.len();

    loop {
        match src.get(*cursor).copied()? {
            b'"' => {
                *cursor += 1;
                std::str::from_utf8(&out[value_offset..]).ok()?;
                let length = u32::try_from(out.len().checked_sub(value_offset)?).ok()?;
                out[length_offset..value_offset].copy_from_slice(&length.to_be_bytes());
                return Some(());
            }
            b'\\' => {
                *cursor += 1;
                match src.get(*cursor).copied()? {
                    b'"' | b'\\' | b'/' => {
                        out.push(src[*cursor]);
                        *cursor += 1;
                    }
                    b'b' => {
                        out.push(0x08);
                        *cursor += 1;
                    }
                    b'f' => {
                        out.push(0x0c);
                        *cursor += 1;
                    }
                    b'n' => {
                        out.push(b'\n');
                        *cursor += 1;
                    }
                    b'r' => {
                        out.push(b'\r');
                        *cursor += 1;
                    }
                    b't' => {
                        out.push(b'\t');
                        *cursor += 1;
                    }
                    b'u' => {
                        let high = decode_hex_u16(src.get(*cursor + 1..*cursor + 5)?)?;
                        *cursor += 5;
                        let scalar = if (0xd800..=0xdbff).contains(&high) {
                            if src.get(*cursor..*cursor + 2)? != b"\\u" {
                                return None;
                            }
                            let low = decode_hex_u16(src.get(*cursor + 2..*cursor + 6)?)?;
                            if !(0xdc00..=0xdfff).contains(&low) {
                                return None;
                            }
                            *cursor += 6;
                            0x1_0000 + ((u32::from(high) - 0xd800) << 10) + (u32::from(low) - 0xdc00)
                        } else if (0xdc00..=0xdfff).contains(&high) {
                            return None;
                        } else {
                            u32::from(high)
                        };
                        let value = char::from_u32(scalar)?;
                        let mut encoded = [0; 4];
                        out.extend_from_slice(value.encode_utf8(&mut encoded).as_bytes());
                    }
                    _ => return None,
                }
            }
            0..=0x1f => return None,
            byte => {
                out.push(byte);
                *cursor += 1;
            }
        }
    }
}

fn decode_length_prefixed_fields(src: &[u8], entry_count: usize, capacity: usize) -> Option<Vec<u8>> {
    let mut cursor = 0usize;
    let mut payload = Vec::with_capacity(capacity);
    for index in 0..entry_count {
        if index != 0 {
            if src.get(cursor).copied()? != b',' {
                return None;
            }
            cursor += 1;
        }
        append_json_string(src, &mut cursor, &mut payload)?;
        if src.get(cursor).copied()? != b':' {
            return None;
        }
        cursor += 1;
        append_json_string(src, &mut cursor, &mut payload)?;
    }
    (cursor == src.len()).then_some(payload)
}

#[inline]
fn validate_optional_string(src: &[u8], range: &Option<Range<usize>>) -> Option<()> {
    if let Some(range) = range {
        std::str::from_utf8(&src[range.clone()]).ok()?;
    }
    Some(())
}

#[inline]
fn mark_seen(seen: &mut u8, field: u8) -> Option<()> {
    if *seen & field != 0 {
        return None;
    }
    *seen |= field;
    Some(())
}

#[inline]
fn language(value: &[u8]) -> Option<LanguageCode> {
    Some(match value {
        b"JAVA" => LanguageCode::JAVA,
        b"CPP" => LanguageCode::CPP,
        b"DOTNET" => LanguageCode::DOTNET,
        b"PYTHON" => LanguageCode::PYTHON,
        b"DELPHI" => LanguageCode::DELPHI,
        b"ERLANG" => LanguageCode::ERLANG,
        b"RUBY" => LanguageCode::RUBY,
        b"OTHER" => LanguageCode::OTHER,
        b"HTTP" => LanguageCode::HTTP,
        b"GO" => LanguageCode::GO,
        b"PHP" => LanguageCode::PHP,
        b"OMS" => LanguageCode::OMS,
        b"RUST" => LanguageCode::RUST,
        b"NODE_JS" => LanguageCode::NODE_JS,
        _ => return None,
    })
}

#[inline]
fn serialize_type(value: &[u8]) -> Option<SerializeType> {
    match value {
        b"JSON" => Some(SerializeType::JSON),
        b"ROCKETMQ" => Some(SerializeType::ROCKETMQ),
        _ => None,
    }
}

struct ParsedJsonHeader {
    code: i32,
    language: LanguageCode,
    version: i32,
    opaque: i32,
    flag: i32,
    remark: Option<Range<usize>>,
    ext_fields: Option<ParsedExtensionFields>,
    serialize_type: SerializeType,
}

enum ParsedExtensionFields {
    Borrowed {
        range: Range<usize>,
        entry_count: usize,
        canonical: bool,
    },
    LengthPrefixed {
        payload: Vec<u8>,
        entry_count: usize,
    },
}

impl ParsedJsonHeader {
    fn into_command(self, src: Bytes) -> Option<RemotingCommand> {
        let remark = match self.remark {
            Some(range) => Some(CheetahString::from_slice(std::str::from_utf8(src.get(range)?).ok()?)),
            None => None,
        };
        let ext_fields = match self.ext_fields {
            Some(fields) => {
                let fields = match fields {
                    ParsedExtensionFields::Borrowed {
                        range,
                        entry_count,
                        canonical: true,
                    } => {
                        std::str::from_utf8(src.get(range.clone())?).ok()?;
                        JsonHeaderFields::from_canonical_unescaped_object(src.slice(range), entry_count)
                    }
                    ParsedExtensionFields::Borrowed {
                        range,
                        entry_count,
                        canonical: false,
                    } => {
                        std::str::from_utf8(src.get(range.clone())?).ok()?;
                        JsonHeaderFields::from_unescaped_object(src.slice(range), entry_count)
                    }
                    ParsedExtensionFields::LengthPrefixed { payload, entry_count } => {
                        JsonHeaderFields::from_length_prefixed(payload, entry_count)
                    }
                };
                ExtensionFields::from_json_raw(fields)
            }
            None => ExtensionFields::default(),
        };

        Some(RemotingCommand {
            code: self.code,
            language: self.language,
            version: self.version,
            opaque: self.opaque,
            flag: self.flag,
            remark,
            ext_fields,
            body: None,
            suspended: false,
            command_custom_header: None,
            custom_header_to_net: false,
            serialize_type: self.serialize_type,
        })
    }
}

#[inline]
fn parse_canonical_nullable_string(parser: &mut JsonHeaderParser<'_>) -> Option<Option<Range<usize>>> {
    if parser.src.get(parser.cursor..)?.starts_with(b"null") {
        parser.cursor += 4;
        Some(None)
    } else {
        parser.parse_canonical_string().map(Some)
    }
}

#[inline]
fn parse_canonical_nullable_fields(parser: &mut JsonHeaderParser<'_>) -> Option<Option<ParsedExtensionFields>> {
    if parser.src.get(parser.cursor..)?.starts_with(b"null") {
        parser.cursor += 4;
        Some(None)
    } else {
        parser.parse_canonical_extension_fields().map(Some)
    }
}

fn try_parse_rust_layout(src: &[u8]) -> Option<ParsedJsonHeader> {
    let mut parser = JsonHeaderParser::new(src);
    parser.consume_literal(b"{\"code\":")?;
    let code = parser.parse_i32()?;
    parser.consume_literal(b",\"language\":")?;
    let language_range = parser.parse_canonical_string()?;
    let language = language(&src[language_range])?;
    parser.consume_literal(b",\"version\":")?;
    let version = parser.parse_i32()?;
    parser.consume_literal(b",\"opaque\":")?;
    let opaque = parser.parse_i32()?;
    parser.consume_literal(b",\"flag\":")?;
    let flag = parser.parse_i32()?;
    parser.consume_literal(b",\"remark\":")?;
    let remark = parse_canonical_nullable_string(&mut parser)?;
    parser.consume_literal(b",\"extFields\":")?;
    let ext_fields = parse_canonical_nullable_fields(&mut parser)?;
    parser.consume_literal(b",\"serializeTypeCurrentRPC\":")?;
    let serialize_range = parser.parse_canonical_string()?;
    let serialize_type = serialize_type(&src[serialize_range])?;
    parser.consume_literal(b"}")?;
    if parser.cursor != src.len() {
        return None;
    }
    validate_optional_string(src, &remark)?;

    Some(ParsedJsonHeader {
        code,
        language,
        version,
        opaque,
        flag,
        remark,
        ext_fields,
        serialize_type,
    })
}

fn try_parse_java_layout(src: &[u8]) -> Option<ParsedJsonHeader> {
    let mut parser = JsonHeaderParser::new(src);
    parser.consume_literal(b"{\"code\":")?;
    let code = parser.parse_i32()?;
    parser.consume_literal(b",\"extFields\":")?;
    let ext_fields = parse_canonical_nullable_fields(&mut parser)?;
    parser.consume_literal(b",\"flag\":")?;
    let flag = parser.parse_i32()?;
    parser.consume_literal(b",\"language\":")?;
    let language_range = parser.parse_canonical_string()?;
    let language = language(&src[language_range])?;
    parser.consume_literal(b",\"opaque\":")?;
    let opaque = parser.parse_i32()?;
    let remark = if parser.src.get(parser.cursor..)?.starts_with(b",\"remark\":") {
        parser.consume_literal(b",\"remark\":")?;
        parse_canonical_nullable_string(&mut parser)?
    } else {
        None
    };
    parser.consume_literal(b",\"serializeTypeCurrentRPC\":")?;
    let serialize_range = parser.parse_canonical_string()?;
    let serialize_type = serialize_type(&src[serialize_range])?;
    parser.consume_literal(b",\"version\":")?;
    let version = parser.parse_i32()?;
    parser.consume_literal(b"}")?;
    if parser.cursor != src.len() {
        return None;
    }
    validate_optional_string(src, &remark)?;

    Some(ParsedJsonHeader {
        code,
        language,
        version,
        opaque,
        flag,
        remark,
        ext_fields,
        serialize_type,
    })
}

fn try_parse_json_header(src: &[u8]) -> Option<ParsedJsonHeader> {
    try_parse_rust_layout(src)
        .or_else(|| try_parse_java_layout(src))
        .or_else(|| try_parse_flexible_json_header(src))
}

#[cold]
#[inline(never)]
fn try_parse_flexible_json_header(src: &[u8]) -> Option<ParsedJsonHeader> {
    let mut parser = JsonHeaderParser::new(src);
    parser.consume(b'{')?;

    let mut seen = 0u8;
    let mut code = 0;
    let mut language_code = LanguageCode::RUST;
    let mut version = 0;
    let mut opaque = 0;
    let mut flag = 0;
    let mut remark = None;
    let mut ext_fields = None;
    let mut rpc_serialize_type = SerializeType::JSON;

    parser.skip_whitespace();
    if parser.src.get(parser.cursor) == Some(&b'}') {
        return None;
    }

    loop {
        let key = parser.parse_string()?;
        parser.consume(b':')?;
        match &parser.src[key] {
            b"code" => {
                mark_seen(&mut seen, CODE)?;
                code = parser.parse_i32()?;
            }
            b"language" => {
                mark_seen(&mut seen, LANGUAGE)?;
                let value = parser.parse_string()?;
                language_code = language(&parser.src[value])?;
            }
            b"version" => {
                mark_seen(&mut seen, VERSION)?;
                version = parser.parse_i32()?;
            }
            b"opaque" => {
                mark_seen(&mut seen, OPAQUE)?;
                opaque = parser.parse_i32()?;
            }
            b"flag" => {
                mark_seen(&mut seen, FLAG)?;
                flag = parser.parse_i32()?;
            }
            b"remark" => {
                mark_seen(&mut seen, REMARK)?;
                parser.skip_whitespace();
                if parser.src.get(parser.cursor..)?.starts_with(b"null") {
                    parser.consume_null()?;
                } else {
                    remark = Some(parser.parse_string()?);
                }
            }
            b"extFields" => {
                mark_seen(&mut seen, EXT_FIELDS)?;
                parser.skip_whitespace();
                if parser.src.get(parser.cursor..)?.starts_with(b"null") {
                    parser.consume_null()?;
                } else {
                    ext_fields = Some(parser.parse_extension_fields()?);
                }
            }
            b"serializeTypeCurrentRPC" => {
                mark_seen(&mut seen, SERIALIZE_TYPE)?;
                let value = parser.parse_string()?;
                rpc_serialize_type = serialize_type(&parser.src[value])?;
            }
            _ => return None,
        }

        parser.skip_whitespace();
        match parser.src.get(parser.cursor).copied()? {
            b',' => parser.cursor += 1,
            b'}' => {
                parser.cursor += 1;
                break;
            }
            _ => return None,
        }
    }

    parser.skip_whitespace();
    if parser.cursor != parser.src.len() || seen & REQUIRED_FIELDS != REQUIRED_FIELDS {
        return None;
    }
    validate_optional_string(src, &remark)?;
    if let Some((range, _)) = &ext_fields {
        std::str::from_utf8(&src[range.clone()]).ok()?;
    }

    Some(ParsedJsonHeader {
        code,
        language: language_code,
        version,
        opaque,
        flag,
        remark,
        ext_fields: ext_fields.map(|(range, entry_count)| ParsedExtensionFields::Borrowed {
            range,
            entry_count,
            canonical: false,
        }),
        serialize_type: rpc_serialize_type,
    })
}

pub(super) fn try_decode_json_header(src: &mut BytesMut, header_length: usize) -> Option<RemotingCommand> {
    let parsed = try_parse_json_header(src.get(..header_length)?)?;
    let header = src.split_to(header_length).freeze();
    parsed.into_command(header)
}

pub(super) fn try_decode_json_header_bytes(header: Bytes) -> Option<RemotingCommand> {
    let parsed = try_parse_json_header(&header)?;
    parsed.into_command(header)
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use bytes::BytesMut;
    use cheetah_string::CheetahString;

    use super::*;
    use crate::protocol::LanguageCode;

    fn decode(input: &[u8]) -> Option<RemotingCommand> {
        let mut input = BytesMut::from(input);
        let length = input.len();
        try_decode_json_header(&mut input, length)
    }

    #[test]
    fn decodes_java_and_rust_canonical_layouts_without_materializing_fields() {
        let headers = [
            br#"{"code":10,"extFields":{"queueId":"1","msgId":"id"},"flag":0,"language":"RUST","opaque":7,"serializeTypeCurrentRPC":"JSON","version":501}"#.as_slice(),
            br#"{"code":10,"language":"RUST","version":501,"opaque":7,"flag":0,"remark":null,"extFields":{"queueId":"1","msgId":"id"},"serializeTypeCurrentRPC":"JSON"}"#.as_slice(),
        ];

        for input in headers {
            let command = decode(input).expect("canonical JSON should use the fast parser");

            assert_eq!(command.code, 10);
            assert_eq!(command.language, LanguageCode::RUST);
            assert_eq!(command.version, 501);
            assert_eq!(command.opaque, 7);
            assert_eq!(command.flag, 0);
            assert!(command.remark.is_none());
            assert!(command.ext_fields.is_json_raw());
            assert!(!command.ext_fields.has_materialized_map());
            assert_eq!(
                command
                    .ext_fields()
                    .and_then(|fields| fields.get("msgId"))
                    .map(CheetahString::as_str),
                Some("id")
            );
        }
    }

    #[test]
    fn decodes_node_js_language_on_fast_path() {
        let input = br#"{"code":10,"language":"NODE_JS","version":501,"opaque":7,"flag":0,"remark":null,"extFields":null,"serializeTypeCurrentRPC":"JSON"}"#;

        let command = decode(input).expect("NODE_JS JSON should use the fast parser");

        assert_eq!(command.language, LanguageCode::NODE_JS);
        assert_eq!(command.version, 501);
    }

    #[test]
    fn decodes_escaped_canonical_extension_field_values() {
        let input = br#"{"code":310,"language":"RUST","version":501,"opaque":7,"flag":0,"remark":null,"extFields":{"a":"producer-a","i":"KEYS\u0001key-a\u0002","escaped\u004bey":"quote\"slash\\solidus\/","controls":"\b\f\n\r\t","unicode":"\u4e2d\ud83d\ude80"},"serializeTypeCurrentRPC":"JSON"}"#;
        let command = decode(input).expect("canonical escaped JSON should use the fast parser");

        let fields = command
            .ext_fields()
            .expect("escaped extension fields should be retained");
        assert_eq!(fields.get("a").map(CheetahString::as_str), Some("producer-a"));
        assert_eq!(fields.get("i").map(CheetahString::as_str), Some("KEYS\u{1}key-a\u{2}"));
        assert_eq!(
            fields.get("escapedKey").map(CheetahString::as_str),
            Some("quote\"slash\\solidus/")
        );
        assert_eq!(
            fields.get("controls").map(CheetahString::as_str),
            Some("\u{8}\u{c}\n\r\t")
        );
        assert_eq!(fields.get("unicode").map(CheetahString::as_str), Some("中🚀"));
    }

    #[test]
    fn preserves_whitespace_unicode_empty_and_duplicate_extension_fields() {
        let input = r#" { "version" : 501, "code" : -7, "language" : "JAVA", "opaque" : 9, "flag" : 1, "remark" : "火箭", "extFields" : { "empty" : "", "key" : "first", "key" : "最后" }, "serializeTypeCurrentRPC" : "JSON" } "#
            .as_bytes();

        let command = decode(input).expect("supported JSON should use the fast parser");
        let fields = command.ext_fields().expect("extension fields");

        assert_eq!(command.code, -7);
        assert_eq!(command.language, LanguageCode::JAVA);
        assert_eq!(command.remark.as_deref(), Some("火箭"));
        assert_eq!(fields.get("empty").map(CheetahString::as_str), Some(""));
        assert_eq!(fields.get("key").map(CheetahString::as_str), Some("最后"));
    }

    #[test]
    fn serde_fallback_decodes_flexible_whitespace_unicode_and_escaped_extension_fields() {
        let input = r#" { "version" : 501, "code" : -7, "language" : "JAVA", "opaque" : 9, "flag" : 1, "remark" : "火箭", "extFields" : { "escaped\u004bey" : "quote\"slash\\solidus\/", "unicode" : "中🚀" }, "serializeTypeCurrentRPC" : "JSON" } "#
            .as_bytes();
        let mut header = BytesMut::from(input);
        let header_length = header.len();

        let command = RemotingCommand::header_decode(&mut header, header_length, SerializeType::JSON)
            .expect("flexible JSON should use the compatibility fallback")
            .expect("flexible JSON header is complete");
        let fields = command.ext_fields().expect("extension fields");

        assert_eq!(command.code, -7);
        assert_eq!(command.language, LanguageCode::JAVA);
        assert_eq!(command.remark.as_deref(), Some("火箭"));
        assert_eq!(
            fields.get("escapedKey").map(CheetahString::as_str),
            Some("quote\"slash\\solidus/")
        );
        assert_eq!(fields.get("unicode").map(CheetahString::as_str), Some("中🚀"));
    }

    #[test]
    fn keeps_absent_null_and_empty_extension_fields_distinct() {
        let prefix = r#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0"#;
        let suffix = r#","serializeTypeCurrentRPC":"JSON"}"#;
        let inputs = [
            format!("{prefix}{suffix}"),
            format!("{prefix},\"extFields\":null{suffix}"),
            format!("{prefix},\"extFields\":{{}}{suffix}"),
        ];

        let absent = decode(inputs[0].as_bytes()).expect("absent fields");
        let null = decode(inputs[1].as_bytes()).expect("null fields");
        let empty = decode(inputs[2].as_bytes()).expect("empty fields");

        assert!(absent.ext_fields.is_absent());
        assert!(null.ext_fields.is_absent());
        assert!(empty.ext_fields.is_json_raw());
        assert!(empty.ext_fields().expect("empty map").is_empty());
    }

    #[test]
    fn parses_i32_boundaries_without_accepting_overflow() {
        let valid = br#"{"code":-2147483648,"language":"RUST","version":2147483647,"opaque":-0,"flag":-2147483648,"serializeTypeCurrentRPC":"JSON"}"#;
        let command = decode(valid).expect("i32 boundaries should decode");
        assert_eq!(command.code, i32::MIN);
        assert_eq!(command.version, i32::MAX);
        assert_eq!(command.opaque, 0);
        assert_eq!(command.flag, i32::MIN);

        for code in ["2147483648", "-2147483649", "999999999999999999999999"] {
            let input = format!(
                r#"{{"code":{code},"language":"RUST","version":0,"opaque":0,"flag":0,"serializeTypeCurrentRPC":"JSON"}}"#
            );
            assert!(decode(input.as_bytes()).is_none());
        }
    }

    #[test]
    fn command_construction_rejects_an_invalid_retained_range() {
        let input = br#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":"ok","serializeTypeCurrentRPC":"JSON"}"#;
        let mut parsed = try_parse_json_header(input).expect("valid JSON header");
        parsed.remark = Some(input.len()..usize::MAX);

        assert!(parsed.into_command(Bytes::from_static(input)).is_none());
    }

    #[test]
    fn rejects_invalid_utf8_in_retained_strings() {
        let prefix = br#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":""#;
        let suffix = br#"","serializeTypeCurrentRPC":"JSON"}"#;
        let remark = [prefix.as_slice(), &[0xff], suffix.as_slice()].concat();

        let prefix =
            br#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":null,"extFields":{"key":""#;
        let suffix = br#""},"serializeTypeCurrentRPC":"JSON"}"#;
        let field = [prefix.as_slice(), &[0xff], suffix.as_slice()].concat();

        assert!(decode(&remark).is_none());
        assert!(decode(&field).is_none());
    }

    #[test]
    fn rejects_unterminated_extension_field_strings_after_consuming_a_complete_frame() {
        let header =
            br#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"remark":null,"extFields":{"key":"value"#;
        assert_eq!(header.len(), 99);

        let mut frame = BytesMut::with_capacity(107);
        frame.put_i32(103);
        frame.put_i32(RemotingCommand::mark_serialize_type(99, SerializeType::JSON));
        frame.extend_from_slice(header);
        assert_eq!(frame.len(), 107);

        assert!(RemotingCommand::decode(&mut frame).is_err());
        assert!(frame.is_empty());
    }

    #[test]
    fn defers_escaped_unknown_duplicate_and_invalid_shapes_to_serde() {
        let inputs = [
            br#"{"code":1,"language":"R\u0055ST","version":0,"opaque":7,"flag":0,"serializeTypeCurrentRPC":"JSON"}"#.as_slice(),
            br#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"extra":true,"serializeTypeCurrentRPC":"JSON"}"#.as_slice(),
            br#"{"code":1,"code":2,"language":"RUST","version":0,"opaque":7,"flag":0,"serializeTypeCurrentRPC":"JSON"}"#.as_slice(),
            br#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"extFields":{"key":1},"serializeTypeCurrentRPC":"JSON"}"#.as_slice(),
            br#"{"code":1,"language":"RUST","version":0,"opaque":7,"flag":0,"serializeTypeCurrentRPC":"JSON",}"#.as_slice(),
        ];

        for input in inputs {
            let mut source = BytesMut::from(input);
            let before = source.clone();
            let length = source.len();
            assert!(try_decode_json_header(&mut source, length).is_none());
            assert_eq!(source, before);
        }
    }
}
