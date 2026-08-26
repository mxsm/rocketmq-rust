// Copyright 2023 The RocketMQ Rust Authors
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

use std::time::Duration;

use rocketmq_transport::api::v1::AdmissionLimits;
use rocketmq_transport::api::v1::CachedConnectionState;
use rocketmq_transport::api::v1::DefaultRequestProcessor;
use rocketmq_transport::api::v1::FileTransferMode;
use rocketmq_transport::api::v1::FrameLimits;
use rocketmq_transport::api::v1::OneShotTransportClient;
use rocketmq_transport::api::v1::RemotingClient;
use rocketmq_transport::api::v1::RemotingDeserializable;
use rocketmq_transport::api::v1::RemotingSerializable;
use rocketmq_transport::api::v1::RequestDeadline;
use rocketmq_transport::api::v1::RequestProcessor;
use rocketmq_transport::api::v1::ServerConfig;
use rocketmq_transport::api::v1::ServerStartError;
use rocketmq_transport::api::v1::TransportClient;
use rocketmq_transport::api::v1::TransportClientConfig;
use rocketmq_transport::api::v1::TransportServer;

fn assert_serialization_contract<T: RemotingSerializable + RemotingDeserializable>() {}

fn assert_processor_contract<T: RequestProcessor>() {}

fn assert_server_start_error_contract<T: Clone + std::fmt::Debug + std::error::Error>() {}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TokenKind {
    Ident(String),
    ColonColon,
    Semi,
    Star,
    Comma,
    Hash,
    Bang,
    LBrace,
    RBrace,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Other(char),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Token {
    kind: TokenKind,
    offset: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SurfaceError {
    offset: usize,
    message: &'static str,
}

impl std::fmt::Display for SurfaceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} at byte {}", self.message, self.offset)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct PublicUse {
    module_path: String,
    use_tree: String,
}

#[derive(Debug, Eq, PartialEq)]
struct PublicBoundary {
    modules: Vec<String>,
    uses: Vec<PublicUse>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Visibility {
    Private,
    Restricted,
    Public,
}

#[derive(Debug)]
enum Scope {
    Module { name: String, externally_public: bool },
    Other,
}

fn surface_error(offset: usize, message: &'static str) -> SurfaceError {
    SurfaceError { offset, message }
}

fn scan_quoted_literal(bytes: &[u8], quote: usize, message: &'static str) -> Result<usize, SurfaceError> {
    let mut cursor = quote + 1;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'\\' => cursor = cursor.saturating_add(2),
            b'"' => return Ok(cursor + 1),
            _ => cursor += 1,
        }
    }
    Err(surface_error(quote, message))
}

fn raw_string_open(bytes: &[u8], start: usize, prefix_bytes: usize) -> Option<(usize, usize)> {
    let mut cursor = start + prefix_bytes;
    let mut hashes = 0;
    while bytes.get(cursor) == Some(&b'#') {
        hashes += 1;
        cursor += 1;
    }
    (bytes.get(cursor) == Some(&b'"')).then_some((cursor + 1, hashes))
}

fn scan_raw_string(bytes: &[u8], start: usize, content_start: usize, hashes: usize) -> Result<usize, SurfaceError> {
    let mut cursor = content_start;
    while cursor < bytes.len() {
        if bytes[cursor] == b'"' {
            let hashes_end = cursor + 1 + hashes;
            if bytes
                .get(cursor + 1..hashes_end)
                .is_some_and(|closing| closing.iter().all(|byte| *byte == b'#'))
            {
                return Ok(hashes_end);
            }
        }
        cursor += 1;
    }
    Err(surface_error(start, "unterminated raw string literal"))
}

fn char_literal_end(source: &str, quote: usize) -> Option<usize> {
    let bytes = source.as_bytes();
    let content = quote + 1;
    let content_end = if bytes.get(content) == Some(&b'\\') {
        match bytes.get(content + 1) {
            Some(b'u') if bytes.get(content + 2) == Some(&b'{') => {
                content + 3 + bytes.get(content + 3..)?.iter().position(|byte| *byte == b'}')? + 1
            }
            Some(b'x') => content.checked_add(4)?,
            Some(_) => content.checked_add(2)?,
            None => return None,
        }
    } else {
        let character = source.get(content..)?.chars().next()?;
        if matches!(character, '\n' | '\r' | '\'') {
            return None;
        }
        content + character.len_utf8()
    };
    (bytes.get(content_end) == Some(&b'\'')).then_some(content_end + 1)
}

fn is_rust_pattern_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{0085}' | '\u{200e}' | '\u{200f}' | '\u{2028}' | '\u{2029}'
    )
}

fn tokenize(source: &str) -> Result<Vec<Token>, SurfaceError> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut cursor = 0;

    while cursor < bytes.len() {
        let character = source
            .get(cursor..)
            .and_then(|remaining| remaining.chars().next())
            .ok_or_else(|| surface_error(cursor, "tokenizer cursor is not on a UTF-8 boundary"))?;
        if is_rust_pattern_whitespace(character) {
            cursor += character.len_utf8();
            continue;
        }
        if bytes.get(cursor..cursor + 2) == Some(b"//") {
            cursor += 2;
            while cursor < bytes.len() && bytes[cursor] != b'\n' {
                cursor += 1;
            }
            continue;
        }
        if bytes.get(cursor..cursor + 2) == Some(b"/*") {
            let comment_start = cursor;
            let mut depth = 1_usize;
            cursor += 2;
            while cursor < bytes.len() && depth != 0 {
                if bytes.get(cursor..cursor + 2) == Some(b"/*") {
                    depth += 1;
                    cursor += 2;
                } else if bytes.get(cursor..cursor + 2) == Some(b"*/") {
                    depth -= 1;
                    cursor += 2;
                } else {
                    cursor += 1;
                }
            }
            if depth != 0 {
                return Err(surface_error(comment_start, "unterminated block comment"));
            }
            continue;
        }

        if bytes.get(cursor..cursor + 2) == Some(b"br") {
            if let Some((content_start, hashes)) = raw_string_open(bytes, cursor, 2) {
                cursor = scan_raw_string(bytes, cursor, content_start, hashes)?;
                continue;
            }
        }
        if bytes.get(cursor..cursor + 2) == Some(b"cr") {
            if let Some((content_start, hashes)) = raw_string_open(bytes, cursor, 2) {
                cursor = scan_raw_string(bytes, cursor, content_start, hashes)?;
                continue;
            }
        }
        if bytes[cursor] == b'r' {
            if let Some((content_start, hashes)) = raw_string_open(bytes, cursor, 1) {
                cursor = scan_raw_string(bytes, cursor, content_start, hashes)?;
                continue;
            }
        }
        if bytes.get(cursor..cursor + 2) == Some(b"b\"") {
            cursor = scan_quoted_literal(bytes, cursor + 1, "unterminated byte string literal")?;
            continue;
        }
        if bytes[cursor] == b'"' {
            cursor = scan_quoted_literal(bytes, cursor, "unterminated string literal")?;
            continue;
        }
        if bytes.get(cursor..cursor + 2) == Some(b"b'") {
            if let Some(end) = char_literal_end(source, cursor + 1) {
                cursor = end;
                continue;
            }
        }
        if bytes[cursor] == b'\'' {
            if let Some(end) = char_literal_end(source, cursor) {
                cursor = end;
                continue;
            }
        }

        let offset = cursor;
        let kind = if bytes[cursor].is_ascii_alphabetic() || bytes[cursor] == b'_' {
            cursor += 1;
            while cursor < bytes.len() && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_') {
                cursor += 1;
            }
            TokenKind::Ident(source[offset..cursor].to_owned())
        } else if bytes.get(cursor..cursor + 2) == Some(b"::") {
            cursor += 2;
            TokenKind::ColonColon
        } else if character.is_ascii() {
            cursor += 1;
            match character as u8 {
                b';' => TokenKind::Semi,
                b'*' => TokenKind::Star,
                b',' => TokenKind::Comma,
                b'#' => TokenKind::Hash,
                b'!' => TokenKind::Bang,
                b'{' => TokenKind::LBrace,
                b'}' => TokenKind::RBrace,
                b'(' => TokenKind::LParen,
                b')' => TokenKind::RParen,
                b'[' => TokenKind::LBracket,
                b']' => TokenKind::RBracket,
                other => TokenKind::Other(other as char),
            }
        } else {
            cursor += character.len_utf8();
            TokenKind::Other(character)
        };
        tokens.push(Token { kind, offset });
    }

    Ok(tokens)
}

fn closing_delimiter(kind: &TokenKind) -> Option<TokenKind> {
    match kind {
        TokenKind::LBrace => Some(TokenKind::RBrace),
        TokenKind::LParen => Some(TokenKind::RParen),
        TokenKind::LBracket => Some(TokenKind::RBracket),
        _ => None,
    }
}

fn skip_balanced(tokens: &[Token], opening: usize) -> Result<usize, SurfaceError> {
    let mut expected = vec![closing_delimiter(&tokens[opening].kind)
        .ok_or_else(|| surface_error(tokens[opening].offset, "expected an opening delimiter"))?];
    let mut cursor = opening + 1;
    while cursor < tokens.len() {
        if let Some(closing) = closing_delimiter(&tokens[cursor].kind) {
            expected.push(closing);
        } else if matches!(
            tokens[cursor].kind,
            TokenKind::RBrace | TokenKind::RParen | TokenKind::RBracket
        ) {
            if expected.last() != Some(&tokens[cursor].kind) {
                return Err(surface_error(tokens[cursor].offset, "mismatched delimiter"));
            }
            expected.pop();
            if expected.is_empty() {
                return Ok(cursor + 1);
            }
        }
        cursor += 1;
    }
    Err(surface_error(tokens[opening].offset, "unterminated delimiter"))
}

fn skip_attribute(tokens: &[Token], start: usize) -> Result<Option<usize>, SurfaceError> {
    if !matches!(tokens[start].kind, TokenKind::Hash) {
        return Ok(None);
    }
    let mut bracket = start + 1;
    if tokens
        .get(bracket)
        .is_some_and(|token| matches!(token.kind, TokenKind::Bang))
    {
        bracket += 1;
    }
    if tokens
        .get(bracket)
        .is_none_or(|token| !matches!(token.kind, TokenKind::LBracket))
    {
        return Ok(None);
    }
    skip_balanced(tokens, bracket).map(Some)
}

fn is_ident(token: Option<&Token>, expected: &str) -> bool {
    token.is_some_and(|token| matches!(&token.kind, TokenKind::Ident(actual) if actual == expected))
}

fn is_macro_invocation(tokens: &[Token], start: usize) -> bool {
    let mut cursor = start;
    if tokens
        .get(cursor)
        .is_some_and(|token| matches!(token.kind, TokenKind::ColonColon))
    {
        cursor += 1;
    }
    if !tokens
        .get(cursor)
        .is_some_and(|token| matches!(token.kind, TokenKind::Ident(_)))
    {
        return false;
    }
    cursor += 1;
    while tokens
        .get(cursor)
        .is_some_and(|token| matches!(token.kind, TokenKind::ColonColon))
    {
        cursor += 1;
        if !tokens
            .get(cursor)
            .is_some_and(|token| matches!(token.kind, TokenKind::Ident(_)))
        {
            return false;
        }
        cursor += 1;
    }
    tokens
        .get(cursor)
        .is_some_and(|token| matches!(token.kind, TokenKind::Bang))
        && tokens
            .get(cursor + 1)
            .is_some_and(|token| matches!(token.kind, TokenKind::LBrace | TokenKind::LParen | TokenKind::LBracket))
}

fn parse_visibility(tokens: &[Token], start: usize) -> Result<(Visibility, usize), SurfaceError> {
    if !is_ident(tokens.get(start), "pub") {
        return Ok((Visibility::Private, start));
    }
    if tokens
        .get(start + 1)
        .is_some_and(|token| matches!(token.kind, TokenKind::LParen))
    {
        return Ok((Visibility::Restricted, skip_balanced(tokens, start + 1)?));
    }
    Ok((Visibility::Public, start + 1))
}

struct ModuleDeclaration<'a> {
    visibility: Visibility,
    name: &'a str,
    terminator: usize,
    inline: bool,
}

fn parse_module_declaration<'a>(
    tokens: &'a [Token],
    start: usize,
) -> Result<Option<ModuleDeclaration<'a>>, SurfaceError> {
    let (visibility, module) = parse_visibility(tokens, start)?;
    if !is_ident(tokens.get(module), "mod") {
        return Ok(None);
    }
    let name_token = tokens
        .get(module + 1)
        .ok_or_else(|| surface_error(tokens[module].offset, "module declaration is missing a name"))?;
    let TokenKind::Ident(name) = &name_token.kind else {
        return Err(surface_error(name_token.offset, "module declaration is missing a name"));
    };
    let terminator = module + 2;
    let inline = match tokens.get(terminator).map(|token| &token.kind) {
        Some(TokenKind::Semi) => false,
        Some(TokenKind::LBrace) => true,
        Some(_) => {
            return Err(surface_error(
                tokens[terminator].offset,
                "module declaration is missing its terminator",
            ))
        }
        None => {
            return Err(surface_error(
                name_token.offset,
                "module declaration is missing its terminator",
            ))
        }
    };
    Ok(Some(ModuleDeclaration {
        visibility,
        name,
        terminator,
        inline,
    }))
}

fn normalized_use_tree(tokens: &[Token]) -> String {
    let mut normalized = String::new();
    for token in tokens {
        match &token.kind {
            TokenKind::Ident(identifier) => normalized.push_str(identifier),
            TokenKind::ColonColon => normalized.push_str("::"),
            TokenKind::Star => normalized.push('*'),
            TokenKind::Comma => normalized.push(','),
            TokenKind::LBrace => normalized.push('{'),
            TokenKind::RBrace => normalized.push('}'),
            TokenKind::LParen => normalized.push('('),
            TokenKind::RParen => normalized.push(')'),
            TokenKind::LBracket => normalized.push('['),
            TokenKind::RBracket => normalized.push(']'),
            TokenKind::Hash => normalized.push('#'),
            TokenKind::Bang => normalized.push('!'),
            TokenKind::Other(character) => normalized.push(*character),
            TokenKind::Semi => normalized.push(';'),
        }
    }
    normalized
}

struct UseDeclaration {
    visibility: Visibility,
    use_tree: String,
    end: usize,
}

fn parse_use_declaration(tokens: &[Token], start: usize) -> Result<Option<UseDeclaration>, SurfaceError> {
    let (visibility, use_keyword) = parse_visibility(tokens, start)?;
    if !is_ident(tokens.get(use_keyword), "use") {
        return Ok(None);
    }
    let tree_start = use_keyword + 1;
    let mut cursor = tree_start;
    let mut delimiters = Vec::new();
    while cursor < tokens.len() {
        if let Some(closing) = closing_delimiter(&tokens[cursor].kind) {
            delimiters.push(closing);
        } else if matches!(
            tokens[cursor].kind,
            TokenKind::RBrace | TokenKind::RParen | TokenKind::RBracket
        ) {
            if delimiters.last() != Some(&tokens[cursor].kind) {
                return Err(surface_error(tokens[cursor].offset, "mismatched delimiter in use tree"));
            }
            delimiters.pop();
        } else if matches!(tokens[cursor].kind, TokenKind::Semi) && delimiters.is_empty() {
            return Ok(Some(UseDeclaration {
                visibility,
                use_tree: normalized_use_tree(&tokens[tree_start..cursor]),
                end: cursor + 1,
            }));
        }
        cursor += 1;
    }
    Err(surface_error(
        tokens[use_keyword].offset,
        "use declaration is missing its semicolon",
    ))
}

fn module_path(scopes: &[Scope]) -> String {
    scopes
        .iter()
        .filter_map(|scope| match scope {
            Scope::Module { name, .. } => Some(name.as_str()),
            Scope::Other => None,
        })
        .collect::<Vec<_>>()
        .join("::")
}

fn parent_is_externally_public(scopes: &[Scope]) -> bool {
    scopes.iter().all(|scope| {
        matches!(
            scope,
            Scope::Module {
                externally_public: true,
                ..
            }
        )
    })
}

fn inspect_public_boundary(source: &str) -> Result<PublicBoundary, SurfaceError> {
    let tokens = tokenize(source)?;
    let mut modules = Vec::new();
    let mut uses = Vec::new();
    let mut scopes = Vec::new();
    let mut cursor = 0;

    while cursor < tokens.len() {
        if let Some(after_attribute) = skip_attribute(&tokens, cursor)? {
            cursor = after_attribute;
            continue;
        }

        let at_module_scope = scopes.iter().all(|scope| matches!(scope, Scope::Module { .. }));
        if at_module_scope {
            if parent_is_externally_public(&scopes) && is_macro_invocation(&tokens, cursor) {
                return Err(surface_error(
                    tokens[cursor].offset,
                    "macro invocation at an externally public module scope is unsupported",
                ));
            }
            if let Some(declaration) = parse_module_declaration(&tokens, cursor)? {
                let externally_public =
                    parent_is_externally_public(&scopes) && declaration.visibility == Visibility::Public;
                if externally_public {
                    let parent = module_path(&scopes);
                    modules.push(if parent.is_empty() {
                        declaration.name.to_owned()
                    } else {
                        format!("{parent}::{}", declaration.name)
                    });
                }
                if declaration.inline {
                    scopes.push(Scope::Module {
                        name: declaration.name.to_owned(),
                        externally_public,
                    });
                }
                cursor = declaration.terminator + 1;
                continue;
            }
            if let Some(declaration) = parse_use_declaration(&tokens, cursor)? {
                if parent_is_externally_public(&scopes) && declaration.visibility == Visibility::Public {
                    uses.push(PublicUse {
                        module_path: module_path(&scopes),
                        use_tree: declaration.use_tree,
                    });
                }
                cursor = declaration.end;
                continue;
            }
            let (visibility, _) = parse_visibility(&tokens, cursor)?;
            if parent_is_externally_public(&scopes) && visibility == Visibility::Public {
                return Err(surface_error(
                    tokens[cursor].offset,
                    "unsupported public item at an externally public module scope",
                ));
            }
        }

        match tokens[cursor].kind {
            TokenKind::LBrace => scopes.push(Scope::Other),
            TokenKind::RBrace => {
                scopes
                    .pop()
                    .ok_or_else(|| surface_error(tokens[cursor].offset, "unmatched closing brace"))?;
            }
            _ => {}
        }
        cursor += 1;
    }

    if !scopes.is_empty() {
        return Err(surface_error(source.len(), "unclosed brace scope"));
    }
    modules.sort();
    Ok(PublicBoundary { modules, uses })
}

fn validate_public_boundary(boundary: &PublicBoundary) -> Result<(), String> {
    let expected_modules = [
        "api",
        "api::v1",
        "api::v2",
        "benchmark_support",
        "prelude",
        "test_support",
    ];
    if boundary.modules != expected_modules {
        return Err(format!("unexpected public modules: {:?}", boundary.modules));
    }
    let top_level_modules = boundary
        .modules
        .iter()
        .filter(|path| !path.contains("::"))
        .map(String::as_str)
        .collect::<Vec<_>>();
    if top_level_modules != ["api", "benchmark_support", "prelude", "test_support"] {
        return Err(format!("unexpected top-level public modules: {top_level_modules:?}"));
    }
    let v1_modules = boundary
        .modules
        .iter()
        .filter(|path| path.rsplit("::").next() == Some("v1"))
        .map(String::as_str)
        .collect::<Vec<_>>();
    if v1_modules != ["api::v1"] {
        return Err(format!("unexpected public v1 modules: {v1_modules:?}"));
    }
    let v2_modules = boundary
        .modules
        .iter()
        .filter(|path| path.rsplit("::").next() == Some("v2"))
        .map(String::as_str)
        .collect::<Vec<_>>();
    if v2_modules != ["api::v2"] {
        return Err(format!("unexpected public v2 modules: {v2_modules:?}"));
    }
    let expected_uses = [
        PublicUse {
            module_path: "api::v1".to_owned(),
            use_tree: "crate::public_api::*".to_owned(),
        },
        PublicUse {
            module_path: "api::v2".to_owned(),
            use_tree: "crate::public_api_v2::*".to_owned(),
        },
    ];
    if boundary.uses != expected_uses {
        return Err(format!("unexpected public uses: {:?}", boundary.uses));
    }
    Ok(())
}

fn validate_public_api_v2_boundary(boundary: &PublicBoundary) -> Result<(), String> {
    if !boundary.modules.is_empty() {
        return Err(format!("unexpected public v2 modules: {:?}", boundary.modules));
    }
    let expected_uses = [
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::deadline::RequestDeadline".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::dispatch::AuthenticationState".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::dispatch::EmbeddedCaller".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::dispatch::IngressRequestView".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::dispatch::OriginalRequestIdentity".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::dispatch::RemotingRequest".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::dispatch::RequestControlView".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::dispatch::RequestId".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::dispatch::RequestMeta".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::dispatch::RequestOrigin".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::session_view::ProxyInfoSnapshot".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::session_view::SessionId".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::session_view::SessionStateView".to_owned(),
        },
        PublicUse {
            module_path: String::new(),
            use_tree: "crate::session_view::SessionView".to_owned(),
        },
    ];
    if boundary.uses != expected_uses {
        return Err(format!("unexpected public v2 uses: {:?}", boundary.uses));
    }
    Ok(())
}

const CURATED_V2_REEXPORTS: &str = "pub use crate::deadline::RequestDeadline; pub use crate::dispatch::AuthenticationState; pub use crate::dispatch::EmbeddedCaller; pub use crate::dispatch::IngressRequestView; pub use crate::dispatch::OriginalRequestIdentity; pub use crate::dispatch::RemotingRequest; pub use crate::dispatch::RequestControlView; pub use crate::dispatch::RequestId; pub use crate::dispatch::RequestMeta; pub use crate::dispatch::RequestOrigin; pub use crate::session_view::ProxyInfoSnapshot; pub use crate::session_view::SessionId; pub use crate::session_view::SessionStateView; pub use crate::session_view::SessionView;";

#[test]
fn lib_rs_exposes_only_the_curated_versioned_boundary() {
    let boundary = inspect_public_boundary(include_str!("../src/lib.rs")).expect("lib.rs must tokenize and parse");
    validate_public_boundary(&boundary).expect("lib.rs must expose only the curated public boundary");
}

#[test]
fn public_api_v2_exposes_exactly_the_curated_request_and_session_fact_types() {
    let boundary =
        inspect_public_boundary(include_str!("../src/public_api_v2.rs")).expect("public_api_v2.rs must tokenize");

    validate_public_api_v2_boundary(&boundary)
        .expect("public_api_v2.rs must expose only approved request, ingress, and session fact types");
}

#[test]
fn public_api_v2_rejects_unapproved_public_surface() {
    for unapproved_use in [
        "pub use crate::net::channel::Channel;",
        "pub use rocketmq_runtime::OperationContext;",
        "pub use crate::deadline::{RequestDeadline,RequestId};",
        "pub use crate::deadline::*;",
        "pub mod request_model {}",
    ] {
        let source = format!("{CURATED_V2_REEXPORTS} {unapproved_use}");
        let boundary = inspect_public_boundary(&source).expect("adversarial V2 fixture must parse");

        assert!(validate_public_api_v2_boundary(&boundary).is_err());
    }

    let source = r#"
macro_rules! expose_extra { () => { pub use crate::net::channel::Channel; }; }
expose_extra!();
pub use crate::deadline::RequestDeadline;
pub use crate::dispatch::AuthenticationState;
pub use crate::dispatch::EmbeddedCaller;
pub use crate::dispatch::IngressRequestView;
pub use crate::dispatch::OriginalRequestIdentity;
pub use crate::dispatch::RemotingRequest;
pub use crate::dispatch::RequestControlView;
pub use crate::dispatch::RequestId;
pub use crate::dispatch::RequestMeta;
pub use crate::dispatch::RequestOrigin;
pub use crate::session_view::ProxyInfoSnapshot;
pub use crate::session_view::SessionId;
pub use crate::session_view::SessionStateView;
pub use crate::session_view::SessionView;
"#;
    let error = inspect_public_boundary(source).expect_err("public V2 macro invocation must be rejected");

    assert_eq!(
        error.message,
        "macro invocation at an externally public module scope is unsupported"
    );
}

#[test]
fn public_api_v2_rejects_direct_public_items() {
    for unapproved_item in ["pub type Channel = crate::net::channel::Channel;", "pub struct Leaked;"] {
        let source = format!("{CURATED_V2_REEXPORTS} {unapproved_item}");
        let error = inspect_public_boundary(&source).expect_err("direct public V2 item must be rejected");

        assert_eq!(
            error.message,
            "unsupported public item at an externally public module scope"
        );
    }
}

#[test]
fn root_glob_with_attribute_and_comment_split_is_rejected() {
    let boundary = inspect_public_boundary("#[rustfmt::skip] pub /* boundary */ use crate::public_api::*;")
        .expect("fixture must parse");

    assert_eq!(
        boundary.uses,
        [PublicUse {
            module_path: String::new(),
            use_tree: "crate::public_api::*".to_owned(),
        }]
    );
    assert!(validate_public_boundary(&boundary).is_err());
}

#[test]
fn comment_split_public_module_is_detected_and_rejected() {
    let boundary = inspect_public_boundary("pub /* boundary */ mod leaked {}").expect("fixture must parse");

    assert_eq!(boundary.modules, ["leaked"]);
    assert!(validate_public_boundary(&boundary).is_err());
}

#[test]
fn rust_pattern_whitespace_between_public_keywords_is_detected() {
    const LEFT_TO_RIGHT_MARK: char = '\u{200e}';
    let mut encoded = [0_u8; 4];
    assert_eq!(
        LEFT_TO_RIGHT_MARK.encode_utf8(&mut encoded).as_bytes(),
        &[0xe2, 0x80, 0x8e]
    );

    let root_glob_source = format!("pub{LEFT_TO_RIGHT_MARK}use crate::public_api::*;");
    let boundary = inspect_public_boundary(&root_glob_source).expect("root glob fixture must parse");
    assert_eq!(
        boundary.uses,
        [PublicUse {
            module_path: String::new(),
            use_tree: "crate::public_api::*".to_owned(),
        }]
    );
    assert!(validate_public_boundary(&boundary).is_err());

    let public_module_source = format!("pub{LEFT_TO_RIGHT_MARK}mod leaked {{}}");
    let boundary = inspect_public_boundary(&public_module_source).expect("public module fixture must parse");
    assert_eq!(boundary.modules, ["leaked"]);
    assert!(validate_public_boundary(&boundary).is_err());
}

#[test]
fn nested_block_comment_before_forbidden_item_is_skipped() {
    let boundary =
        inspect_public_boundary("/* outer /* nested pub mod fake {} */ still outer */ pub use crate::leaked::*;")
            .expect("fixture must parse");

    assert_eq!(
        boundary.uses,
        [PublicUse {
            module_path: String::new(),
            use_tree: "crate::leaked::*".to_owned(),
        }]
    );
    assert!(validate_public_boundary(&boundary).is_err());
}

#[test]
fn literal_contents_do_not_corrupt_module_context() {
    let source = r####"
const NORMAL: &str = "}\
pub mod fake_normal {}
pub use crate::fake_normal::*; \" pub mod fake_escaped {} {";
const RAW: &str = r###"} pub mod fake_raw {} pub use crate::fake_raw::*; {"###;
const BYTES: &[u8] = b"} pub mod fake_bytes {} {";
const RAW_BYTES: &[u8] = br##"} pub use crate::fake_raw_bytes::*; {"##;
const CHARACTER: char = '}';
const BYTE_CHARACTER: u8 = b'{';
pub mod benchmark_support;
pub mod prelude;
pub mod test_support;
pub mod api { pub mod v1 { pub use crate::public_api::*; } pub mod v2 { pub use crate::public_api_v2::*; } }
"####;
    let boundary = inspect_public_boundary(source).expect("literal fixture must parse");

    validate_public_boundary(&boundary).expect("literal contents must not affect the public boundary");
}

#[test]
fn raw_c_literal_braces_do_not_hide_unexpected_public_module() {
    let source = r#####"
const _: &std::ffi::CStr = cr####"} pub mod fake {} {"####;
const _: &std::ffi::CStr = cr#"" { ""#; pub mod leaked {} const _: &std::ffi::CStr = cr#"" } ""#;
mod public_api {}
mod public_api_v2 {}
pub mod benchmark_support {}
pub mod prelude {}
pub mod test_support {}
pub mod api { pub mod v1 { pub use crate::public_api::*; } pub mod v2 { pub use crate::public_api_v2::*; } }
"#####;
    let boundary = inspect_public_boundary(source).expect("raw C string fixture must parse");

    assert_eq!(
        boundary.modules,
        [
            "api",
            "api::v1",
            "api::v2",
            "benchmark_support",
            "leaked",
            "prelude",
            "test_support"
        ]
    );
    assert_eq!(
        boundary.uses,
        [
            PublicUse {
                module_path: "api::v1".to_owned(),
                use_tree: "crate::public_api::*".to_owned(),
            },
            PublicUse {
                module_path: "api::v2".to_owned(),
                use_tree: "crate::public_api_v2::*".to_owned(),
            },
        ]
    );
    assert!(validate_public_boundary(&boundary).is_err());
}

#[test]
fn root_macro_invocation_that_expands_a_public_module_is_rejected() {
    let source = r#"
macro_rules! expose_module { () => { pub mod leaked {} }; }
expose_module!();
mod public_api {}
mod public_api_v2 {}
pub mod benchmark_support {}
pub mod prelude {}
pub mod test_support {}
pub mod api { pub mod v1 { pub use crate::public_api::*; } pub mod v2 { pub use crate::public_api_v2::*; } }
"#;
    let error = inspect_public_boundary(source).expect_err("root macro invocation must be rejected");

    assert_eq!(
        error.message,
        "macro invocation at an externally public module scope is unsupported"
    );
    assert_eq!(
        error.offset,
        source
            .find("expose_module!();")
            .expect("fixture must contain the invocation")
    );
}

#[test]
fn public_api_macro_invocation_that_expands_a_public_use_is_rejected() {
    let source = r#"
mod hidden { pub struct Hidden; }
macro_rules! expose_hidden { () => { pub use crate::hidden::*; }; }
mod public_api {}
mod public_api_v2 {}
pub mod benchmark_support {}
pub mod prelude {}
pub mod test_support {}
pub mod api { pub mod v1 { expose_hidden!(); pub use crate::public_api::*; } pub mod v2 { pub use crate::public_api_v2::*; } }
"#;
    let error = inspect_public_boundary(source).expect_err("api::v1 macro invocation must be rejected");

    assert_eq!(
        error.message,
        "macro invocation at an externally public module scope is unsupported"
    );
    assert_eq!(
        error.offset,
        source
            .find("expose_hidden!();")
            .expect("fixture must contain the invocation")
    );
}

#[test]
fn macro_definition_and_isolated_private_invocation_do_not_corrupt_public_scope() {
    let source = r#"
macro_rules! private_items { ($($item:item)*) => {}; }
mod isolated { private_items! { pub mod fake {} } }
mod public_api {}
mod public_api_v2 {}
pub mod benchmark_support {}
pub mod prelude {}
pub mod test_support {}
pub mod api { pub mod v1 { pub use crate::public_api::*; } pub mod v2 { pub use crate::public_api_v2::*; } }
"#;
    let boundary = inspect_public_boundary(source).expect("private macro fixture must parse");

    validate_public_boundary(&boundary).expect("isolated private macros must not affect the public boundary");
}

#[test]
fn attributes_and_multiple_items_per_line_are_parsed_structurally() {
    let source = r#"
#[cfg(feature = "test-support")] pub mod benchmark_support; #[doc = "pub mod fake_doc {}"] pub mod prelude;
#[cfg(any(test, feature = "test-support"))] pub mod test_support; #[rustfmt::skip] pub mod api { #[allow(dead_code)] pub mod v1 { pub use crate /* path */ :: public_api /* glob */ :: * ; } pub mod v2 { pub use crate::public_api_v2::*; } }
"#;
    let boundary = inspect_public_boundary(source).expect("attribute fixture must parse");

    validate_public_boundary(&boundary).expect("attributes and item layout must not affect the public boundary");
}

#[test]
fn restricted_visibility_is_not_externally_public() {
    let source = "pub(crate) mod internal; pub(in crate) use crate::internal::*; mod private { pub mod nested {} pub use crate::nested::*; }";
    let boundary = inspect_public_boundary(source).expect("restricted fixture must parse");

    assert!(boundary.modules.is_empty());
    assert!(boundary.uses.is_empty());
}

#[test]
fn valid_nested_versioned_api_globs_are_accepted() {
    let source = "pub mod benchmark_support; pub mod prelude; pub mod test_support; pub mod api { pub mod v1 { pub use crate::public_api::*; } pub mod v2 { pub use crate::public_api_v2::*; } }";
    let boundary = inspect_public_boundary(source).expect("valid boundary fixture must parse");

    validate_public_boundary(&boundary).expect("the expected versioned API boundary must be accepted");
}

#[test]
fn malformed_fixture_returns_a_clear_scanner_error() {
    let error =
        inspect_public_boundary("/* outer /* nested */").expect_err("unterminated nested comment must be rejected");

    assert_eq!(error.message, "unterminated block comment");
}

#[test]
fn api_v1_reexports_versioned_capabilities_and_dtos() {
    let _ = AdmissionLimits::default();
    let _ = FrameLimits::default();
    let _ = ServerConfig::default();
    let _ = ServerConfig {
        listen_port: 10911,
        bind_address: "127.0.0.1".to_owned(),
        tls_config: Default::default(),
        file_transfer_mode: FileTransferMode::Auto,
    };
    let _ = TransportClientConfig {
        connect: Default::default(),
        maintenance: Default::default(),
        tls: Default::default(),
        #[cfg(feature = "socks")]
        socks_proxy: Default::default(),
    };
    let _: Option<OneShotTransportClient> = None;
    let _: Option<TransportClient<DefaultRequestProcessor>> = None;
    let _: Option<RemotingClient<DefaultRequestProcessor>> = None;
    let _: Option<TransportServer<DefaultRequestProcessor>> = None;
    assert_server_start_error_contract::<ServerStartError>();
    let _: CachedConnectionState = CachedConnectionState::Absent;
    let _ = CachedConnectionState::Healthy;
    let _ = CachedConnectionState::UnhealthyRetired;
    let _ = RequestDeadline::after(Duration::from_millis(1));
    assert_serialization_contract::<String>();
    assert_processor_contract::<DefaultRequestProcessor>();
}
