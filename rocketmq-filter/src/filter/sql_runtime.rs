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

use std::cmp::Ordering;
use std::fmt;

use cheetah_string::CheetahString;

use crate::expression::EvaluationContext;
use crate::expression::EvaluationError;
use crate::expression::Expression;
use crate::expression::Value;
use crate::filter::filter_spi::FilterError;

pub(crate) fn compile_expression(expr: &str) -> Result<Box<dyn Expression>, FilterError> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return Err(FilterError::new("empty SQL92 expression"));
    }

    let mut parser = Parser::new(trimmed)?;
    let root = parser.parse_expression()?;
    parser.expect_end()?;

    Ok(Box::new(SqlExpression {
        source: trimmed.to_string(),
        root,
    }))
}

struct SqlExpression {
    source: String,
    root: ExprNode,
}

impl Expression for SqlExpression {
    fn evaluate(&self, context: &dyn EvaluationContext) -> Result<Value, EvaluationError> {
        self.root.evaluate(context)
    }
}

impl fmt::Display for SqlExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.source)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ExprNode {
    Literal(Value),
    Property(CheetahString),
    BooleanCast(Box<ExprNode>),
    Not(Box<ExprNode>),
    Logical {
        op: LogicalOp,
        left: Box<ExprNode>,
        right: Box<ExprNode>,
    },
    Compare {
        op: CompareOp,
        left: Box<ExprNode>,
        right: Box<ExprNode>,
    },
    IsNull {
        expr: Box<ExprNode>,
        negated: bool,
    },
}

impl ExprNode {
    fn evaluate(&self, context: &dyn EvaluationContext) -> Result<Value, EvaluationError> {
        match self {
            ExprNode::Literal(value) => Ok(value.clone()),
            ExprNode::Property(name) => Ok(context
                .get(name.as_str())
                .cloned()
                .map(Value::String)
                .unwrap_or(Value::Null)),
            ExprNode::BooleanCast(expr) => Ok(match expr.evaluate(context)? {
                Value::Null => Value::Null,
                value => match coerce_bool(&value) {
                    Some(boolean) => Value::Boolean(boolean),
                    None => Value::Boolean(false),
                },
            }),
            ExprNode::Not(expr) => Ok(match expr.evaluate_bool(context)? {
                Some(value) => Value::Boolean(!value),
                None => Value::Null,
            }),
            ExprNode::Logical { op, left, right } => Ok(match op.eval(left, right, context)? {
                Some(value) => Value::Boolean(value),
                None => Value::Null,
            }),
            ExprNode::Compare { op, left, right } => Ok(match op.eval(left, right, context)? {
                Some(value) => Value::Boolean(value),
                None => Value::Null,
            }),
            ExprNode::IsNull { expr, negated } => {
                let is_null = matches!(expr.evaluate(context)?, Value::Null);
                Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
            }
        }
    }

    fn evaluate_bool(&self, context: &dyn EvaluationContext) -> Result<Option<bool>, EvaluationError> {
        match self.evaluate(context)? {
            Value::Null => Ok(None),
            value => Ok(coerce_bool(&value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogicalOp {
    And,
    Or,
}

impl LogicalOp {
    fn eval(
        self,
        left: &ExprNode,
        right: &ExprNode,
        context: &dyn EvaluationContext,
    ) -> Result<Option<bool>, EvaluationError> {
        let left = left.evaluate_bool(context)?;
        match self {
            LogicalOp::And => {
                if left == Some(false) {
                    return Ok(Some(false));
                }
                let right = right.evaluate_bool(context)?;
                Ok(match (left, right) {
                    (Some(true), Some(false)) | (None, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    (Some(true), None) | (None, Some(true)) | (None, None) => None,
                    _ => unreachable!("left was already checked for false"),
                })
            }
            LogicalOp::Or => {
                if left == Some(true) {
                    return Ok(Some(true));
                }
                let right = right.evaluate_bool(context)?;
                Ok(match (left, right) {
                    (Some(false), Some(true)) | (None, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    (Some(false), None) | (None, Some(false)) | (None, None) => None,
                    _ => unreachable!("left was already checked for true"),
                })
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompareOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl CompareOp {
    fn eval(
        self,
        left: &ExprNode,
        right: &ExprNode,
        context: &dyn EvaluationContext,
    ) -> Result<Option<bool>, EvaluationError> {
        let left = left.evaluate(context)?;
        let right = right.evaluate(context)?;

        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            return Ok(None);
        }

        if let (Some(left), Some(right)) = (coerce_numeric(&left), coerce_numeric(&right)) {
            return Ok(Some(match self {
                CompareOp::Eq => numeric_eq(left, right),
                CompareOp::Ne => !numeric_eq(left, right),
                CompareOp::Gt => left.partial_cmp(&right) == Some(Ordering::Greater),
                CompareOp::Gte => {
                    matches!(
                        left.partial_cmp(&right),
                        Some(Ordering::Greater) | Some(Ordering::Equal)
                    )
                }
                CompareOp::Lt => left.partial_cmp(&right) == Some(Ordering::Less),
                CompareOp::Lte => {
                    matches!(left.partial_cmp(&right), Some(Ordering::Less) | Some(Ordering::Equal))
                }
            }));
        }

        if let (Some(left), Some(right)) = (coerce_boolean_literal(&left), coerce_boolean_literal(&right)) {
            return Ok(match self {
                CompareOp::Eq => Some(left == right),
                CompareOp::Ne => Some(left != right),
                CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => None,
            });
        }

        let left = stringify_value(&left);
        let right = stringify_value(&right);
        let order = left.cmp(&right);

        Ok(Some(match self {
            CompareOp::Eq => order == Ordering::Equal,
            CompareOp::Ne => order != Ordering::Equal,
            CompareOp::Gt => order == Ordering::Greater,
            CompareOp::Gte => matches!(order, Ordering::Greater | Ordering::Equal),
            CompareOp::Lt => order == Ordering::Less,
            CompareOp::Lte => matches!(order, Ordering::Less | Ordering::Equal),
        }))
    }
}

fn coerce_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(boolean) => Some(*boolean),
        Value::Long(number) => Some(*number != 0),
        Value::Double(number) => Some(*number != 0.0),
        Value::String(string) => {
            if string.eq_ignore_ascii_case("true") {
                Some(true)
            } else if string.eq_ignore_ascii_case("false") {
                Some(false)
            } else {
                None
            }
        }
        Value::Null => None,
    }
}

fn coerce_boolean_literal(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(boolean) => Some(*boolean),
        Value::String(string) if string.eq_ignore_ascii_case("true") => Some(true),
        Value::String(string) if string.eq_ignore_ascii_case("false") => Some(false),
        _ => None,
    }
}

fn coerce_numeric(value: &Value) -> Option<f64> {
    match value {
        Value::Long(number) => Some(*number as f64),
        Value::Double(number) => Some(*number),
        Value::String(string) => {
            if let Ok(number) = string.parse::<i64>() {
                Some(number as f64)
            } else {
                string.parse::<f64>().ok()
            }
        }
        _ => None,
    }
}

fn numeric_eq(left: f64, right: f64) -> bool {
    (left - right).abs() <= f64::EPSILON
}

fn stringify_value(value: &Value) -> String {
    match value {
        Value::Boolean(boolean) => boolean.to_string(),
        Value::String(string) => string.to_string(),
        Value::Long(number) => number.to_string(),
        Value::Double(number) => number.to_string(),
        Value::Null => "null".to_string(),
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(CheetahString),
    String(CheetahString),
    Long(i64),
    Double(f64),
    Boolean(bool),
    Null,
    LParen,
    RParen,
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
    Not,
    Is,
    End,
}

struct Lexer<'a> {
    input: &'a str,
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            cursor: 0,
        }
    }

    fn next_token(&mut self) -> Result<Token, FilterError> {
        self.skip_whitespace();
        if self.cursor >= self.bytes.len() {
            return Ok(Token::End);
        }

        let byte = self.bytes[self.cursor];
        match byte {
            b'(' => {
                self.cursor += 1;
                Ok(Token::LParen)
            }
            b')' => {
                self.cursor += 1;
                Ok(Token::RParen)
            }
            b'=' => {
                self.cursor += 1;
                Ok(Token::Eq)
            }
            b'!' => {
                if self.peek_byte(1) == Some(b'=') {
                    self.cursor += 2;
                    Ok(Token::Ne)
                } else {
                    Err(FilterError::new(format!(
                        "unexpected token '!' at position {}",
                        self.cursor
                    )))
                }
            }
            b'<' => {
                if self.peek_byte(1) == Some(b'=') {
                    self.cursor += 2;
                    Ok(Token::Lte)
                } else if self.peek_byte(1) == Some(b'>') {
                    self.cursor += 2;
                    Ok(Token::Ne)
                } else {
                    self.cursor += 1;
                    Ok(Token::Lt)
                }
            }
            b'>' => {
                if self.peek_byte(1) == Some(b'=') {
                    self.cursor += 2;
                    Ok(Token::Gte)
                } else {
                    self.cursor += 1;
                    Ok(Token::Gt)
                }
            }
            b'\'' => self.read_string(),
            b'-' if self.peek_byte(1).is_some_and(|next| next.is_ascii_digit()) => self.read_number(),
            b'0'..=b'9' => self.read_number(),
            _ if is_ident_start(byte) => self.read_identifier(),
            _ => Err(FilterError::new(format!(
                "unexpected token '{}' at position {}",
                self.bytes[self.cursor] as char, self.cursor
            ))),
        }
    }

    fn read_string(&mut self) -> Result<Token, FilterError> {
        self.cursor += 1;
        let mut string = String::new();
        while self.cursor < self.bytes.len() {
            let byte = self.bytes[self.cursor];
            if byte == b'\'' {
                if self.peek_byte(1) == Some(b'\'') {
                    string.push('\'');
                    self.cursor += 2;
                    continue;
                }
                self.cursor += 1;
                return Ok(Token::String(CheetahString::from_string(string)));
            }

            string.push(byte as char);
            self.cursor += 1;
        }

        Err(FilterError::new("unterminated string literal"))
    }

    fn read_number(&mut self) -> Result<Token, FilterError> {
        let start = self.cursor;
        self.cursor += 1;
        while self.cursor < self.bytes.len() && self.bytes[self.cursor].is_ascii_digit() {
            self.cursor += 1;
        }

        let mut is_double = false;
        if self.cursor < self.bytes.len() && self.bytes[self.cursor] == b'.' {
            is_double = true;
            self.cursor += 1;
            while self.cursor < self.bytes.len() && self.bytes[self.cursor].is_ascii_digit() {
                self.cursor += 1;
            }
        }

        let token = &self.input[start..self.cursor];
        if is_double {
            token
                .parse::<f64>()
                .map(Token::Double)
                .map_err(|error| FilterError::new(format!("invalid number '{token}': {error}")))
        } else {
            token
                .parse::<i64>()
                .map(Token::Long)
                .map_err(|error| FilterError::new(format!("invalid number '{token}': {error}")))
        }
    }

    fn read_identifier(&mut self) -> Result<Token, FilterError> {
        let start = self.cursor;
        self.cursor += 1;
        while self.cursor < self.bytes.len() && is_ident_part(self.bytes[self.cursor]) {
            self.cursor += 1;
        }

        let ident = &self.input[start..self.cursor];
        let upper = ident.to_ascii_uppercase();
        match upper.as_str() {
            "AND" => Ok(Token::And),
            "OR" => Ok(Token::Or),
            "NOT" => Ok(Token::Not),
            "IS" => Ok(Token::Is),
            "TRUE" => Ok(Token::Boolean(true)),
            "FALSE" => Ok(Token::Boolean(false)),
            "NULL" => Ok(Token::Null),
            _ => Ok(Token::Ident(CheetahString::from_slice(ident))),
        }
    }

    fn peek_byte(&self, offset: usize) -> Option<u8> {
        self.bytes.get(self.cursor + offset).copied()
    }

    fn skip_whitespace(&mut self) {
        while self.cursor < self.bytes.len() && self.bytes[self.cursor].is_ascii_whitespace() {
            self.cursor += 1;
        }
    }
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$')
}

fn is_ident_part(byte: u8) -> bool {
    is_ident_start(byte) || byte.is_ascii_digit() || matches!(byte, b'.')
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
}

impl Parser {
    fn new(expr: &str) -> Result<Self, FilterError> {
        let mut lexer = Lexer::new(expr);
        let mut tokens = Vec::new();
        loop {
            let token = lexer.next_token()?;
            let is_end = token == Token::End;
            tokens.push(token);
            if is_end {
                break;
            }
        }

        Ok(Self { tokens, cursor: 0 })
    }

    fn parse_expression(&mut self) -> Result<ExprNode, FilterError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<ExprNode, FilterError> {
        let mut expr = self.parse_and()?;
        while self.consume(TokenKind::Or) {
            let right = self.parse_and()?;
            expr = ExprNode::Logical {
                op: LogicalOp::Or,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_and(&mut self) -> Result<ExprNode, FilterError> {
        let mut expr = self.parse_not()?;
        while self.consume(TokenKind::And) {
            let right = self.parse_not()?;
            expr = ExprNode::Logical {
                op: LogicalOp::And,
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    fn parse_not(&mut self) -> Result<ExprNode, FilterError> {
        if self.consume(TokenKind::Not) {
            return Ok(ExprNode::Not(Box::new(self.parse_not()?)));
        }

        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<ExprNode, FilterError> {
        if self.consume(TokenKind::LParen) {
            let expr = self.parse_expression()?;
            self.expect(TokenKind::RParen)?;
            return Ok(expr);
        }

        let left = self.parse_value()?;
        if self.consume(TokenKind::Is) {
            let negated = self.consume(TokenKind::Not);
            self.expect(TokenKind::Null)?;
            return Ok(ExprNode::IsNull {
                expr: Box::new(left),
                negated,
            });
        }

        if let Some(op) = self.parse_compare_op() {
            let right = self.parse_value()?;
            return Ok(ExprNode::Compare {
                op,
                left: Box::new(left),
                right: Box::new(right),
            });
        }

        Ok(ExprNode::BooleanCast(Box::new(left)))
    }

    fn parse_value(&mut self) -> Result<ExprNode, FilterError> {
        match self.next() {
            Token::Ident(name) => Ok(ExprNode::Property(name)),
            Token::String(value) => Ok(ExprNode::Literal(Value::String(value))),
            Token::Long(value) => Ok(ExprNode::Literal(Value::Long(value))),
            Token::Double(value) => Ok(ExprNode::Literal(Value::Double(value))),
            Token::Boolean(value) => Ok(ExprNode::Literal(Value::Boolean(value))),
            Token::Null => Ok(ExprNode::Literal(Value::Null)),
            token => Err(FilterError::new(format!("unexpected token {token:?}"))),
        }
    }

    fn parse_compare_op(&mut self) -> Option<CompareOp> {
        if self.consume(TokenKind::Eq) {
            Some(CompareOp::Eq)
        } else if self.consume(TokenKind::Ne) {
            Some(CompareOp::Ne)
        } else if self.consume(TokenKind::Gte) {
            Some(CompareOp::Gte)
        } else if self.consume(TokenKind::Gt) {
            Some(CompareOp::Gt)
        } else if self.consume(TokenKind::Lte) {
            Some(CompareOp::Lte)
        } else if self.consume(TokenKind::Lt) {
            Some(CompareOp::Lt)
        } else {
            None
        }
    }

    fn expect_end(&self) -> Result<(), FilterError> {
        match self.peek() {
            Token::End => Ok(()),
            token => Err(FilterError::new(format!("unexpected trailing token {token:?}"))),
        }
    }

    fn expect(&mut self, kind: TokenKind) -> Result<(), FilterError> {
        if self.consume(kind) {
            Ok(())
        } else {
            Err(FilterError::new(format!("expected {}", kind.as_str())))
        }
    }

    fn consume(&mut self, kind: TokenKind) -> bool {
        if kind.matches(self.peek()) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn next(&mut self) -> Token {
        let token = self.peek().clone();
        self.cursor += 1;
        token
    }

    fn peek(&self) -> &Token {
        self.tokens
            .get(self.cursor)
            .unwrap_or_else(|| self.tokens.last().expect("parser always has EOF"))
    }
}

#[derive(Debug, Clone, Copy)]
enum TokenKind {
    LParen,
    RParen,
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
    Not,
    Is,
    Null,
}

impl TokenKind {
    fn matches(self, token: &Token) -> bool {
        matches!(
            (self, token),
            (TokenKind::LParen, Token::LParen)
                | (TokenKind::RParen, Token::RParen)
                | (TokenKind::Eq, Token::Eq)
                | (TokenKind::Ne, Token::Ne)
                | (TokenKind::Gt, Token::Gt)
                | (TokenKind::Gte, Token::Gte)
                | (TokenKind::Lt, Token::Lt)
                | (TokenKind::Lte, Token::Lte)
                | (TokenKind::And, Token::And)
                | (TokenKind::Or, Token::Or)
                | (TokenKind::Not, Token::Not)
                | (TokenKind::Is, Token::Is)
                | (TokenKind::Null, Token::Null)
        )
    }

    fn as_str(self) -> &'static str {
        match self {
            TokenKind::LParen => "(",
            TokenKind::RParen => ")",
            TokenKind::Eq => "=",
            TokenKind::Ne => "<>",
            TokenKind::Gt => ">",
            TokenKind::Gte => ">=",
            TokenKind::Lt => "<",
            TokenKind::Lte => "<=",
            TokenKind::And => "AND",
            TokenKind::Or => "OR",
            TokenKind::Not => "NOT",
            TokenKind::Is => "IS",
            TokenKind::Null => "NULL",
        }
    }
}

#[cfg(test)]
mod tests {
    use ahash::RandomState;
    use std::collections::HashMap;

    use super::*;
    use crate::expression::MessageEvaluationContext;

    fn evaluate(expr: &str, properties: &[(&str, &str)]) -> Value {
        let compiled = compile_expression(expr).expect("expression should compile");
        let mut map = HashMap::with_hasher(RandomState::default());
        for (key, value) in properties {
            map.insert(CheetahString::from_slice(key), CheetahString::from_slice(value));
        }
        let context = MessageEvaluationContext::from_properties(map);
        compiled.evaluate(&context).expect("evaluation should succeed")
    }

    #[test]
    fn compile_rejects_invalid_sql() {
        assert!(compile_expression("a =").is_err());
    }

    #[test]
    fn evaluate_string_and_numeric_comparisons() {
        let value = evaluate(
            "color = 'blue' AND retries >= 3",
            &[("color", "blue"), ("retries", "3")],
        );
        assert_eq!(value, Value::Boolean(true));
    }

    #[test]
    fn evaluate_not_and_parentheses() {
        let value = evaluate(
            "NOT (color = 'red' OR retries < 2)",
            &[("color", "blue"), ("retries", "3")],
        );
        assert_eq!(value, Value::Boolean(true));
    }

    #[test]
    fn evaluate_is_null() {
        let value = evaluate("missing IS NULL", &[("color", "blue")]);
        assert_eq!(value, Value::Boolean(true));
    }

    #[test]
    fn evaluate_boolean_literals() {
        let value = evaluate("TRUE AND NOT FALSE", &[]);
        assert_eq!(value, Value::Boolean(true));
    }
}
