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
use rocketmq_common::TimeUtils::current_millis;

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
    Now,
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
    InList {
        expr: Box<ExprNode>,
        items: Vec<Value>,
        negated: bool,
    },
    Between {
        expr: Box<ExprNode>,
        low: Box<ExprNode>,
        high: Box<ExprNode>,
        negated: bool,
    },
    StringMatch {
        op: StringMatchOp,
        expr: Box<ExprNode>,
        search: CheetahString,
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
            ExprNode::Now => Ok(Value::Long(current_millis() as i64)),
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
            ExprNode::Compare { op, left, right } => {
                let left = left.evaluate(context)?;
                let right = right.evaluate(context)?;
                Ok(match eval_compare_values(*op, &left, &right)? {
                    Some(value) => Value::Boolean(value),
                    None => Value::Null,
                })
            }
            ExprNode::IsNull { expr, negated } => {
                let is_null = matches!(expr.evaluate(context)?, Value::Null);
                Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
            }
            ExprNode::InList { expr, items, negated } => {
                let value = expr.evaluate(context)?;
                if matches!(value, Value::Null) {
                    return Ok(Value::Null);
                }

                let mut matched = false;
                for item in items {
                    if eval_compare_values(CompareOp::Eq, &value, item)? == Some(true) {
                        matched = true;
                        break;
                    }
                }

                Ok(Value::Boolean(if *negated { !matched } else { matched }))
            }
            ExprNode::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let value = expr.evaluate(context)?;
                let low = low.evaluate(context)?;
                let high = high.evaluate(context)?;

                let result = if *negated {
                    let lower = eval_compare_values(CompareOp::Lt, &value, &low)?;
                    let upper = eval_compare_values(CompareOp::Gt, &value, &high)?;
                    match (lower, upper) {
                        (Some(true), _) | (_, Some(true)) => Some(true),
                        (Some(false), Some(false)) => Some(false),
                        _ => None,
                    }
                } else {
                    let lower = eval_compare_values(CompareOp::Gte, &value, &low)?;
                    let upper = eval_compare_values(CompareOp::Lte, &value, &high)?;
                    match (lower, upper) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    }
                };

                Ok(match result {
                    Some(value) => Value::Boolean(value),
                    None => Value::Null,
                })
            }
            ExprNode::StringMatch {
                op,
                expr,
                search,
                negated,
            } => {
                if search.is_empty() {
                    return Ok(Value::Boolean(false));
                }

                let Value::String(subject) = expr.evaluate(context)? else {
                    return Ok(Value::Boolean(false));
                };

                let matched = match op {
                    StringMatchOp::Contains => subject.contains(search.as_str()),
                    StringMatchOp::StartsWith => subject.starts_with(search.as_str()),
                    StringMatchOp::EndsWith => subject.ends_with(search.as_str()),
                };

                Ok(Value::Boolean(if *negated { !matched } else { matched }))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringMatchOp {
    Contains,
    StartsWith,
    EndsWith,
}

fn eval_compare_values(op: CompareOp, left: &Value, right: &Value) -> Result<Option<bool>, EvaluationError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(None);
    }

    if let (Some(left), Some(right)) = (coerce_numeric(left), coerce_numeric(right)) {
        let ordering = left.partial_cmp(&right).ok_or_else(|| {
            EvaluationError::InvalidOperation(CheetahString::from_static_str(
                "numeric comparison returned no ordering",
            ))
        })?;

        return Ok(Some(match op {
            CompareOp::Eq => numeric_eq(left, right),
            CompareOp::Ne => !numeric_eq(left, right),
            CompareOp::Gt => ordering == Ordering::Greater,
            CompareOp::Gte => matches!(ordering, Ordering::Greater | Ordering::Equal),
            CompareOp::Lt => ordering == Ordering::Less,
            CompareOp::Lte => matches!(ordering, Ordering::Less | Ordering::Equal),
        }));
    }

    if let (Some(left), Some(right)) = (coerce_boolean_literal(left), coerce_boolean_literal(right)) {
        return match op {
            CompareOp::Eq => Ok(Some(left == right)),
            CompareOp::Ne => Ok(Some(left != right)),
            CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => Err(EvaluationError::InvalidOperation(
                CheetahString::from_static_str("boolean ordering comparison is not supported"),
            )),
        };
    }

    match op {
        CompareOp::Eq => Ok(Some(stringify_value(left) == stringify_value(right))),
        CompareOp::Ne => Ok(Some(stringify_value(left) != stringify_value(right))),
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => Err(EvaluationError::InvalidOperation(
            CheetahString::from_static_str("ordering comparison requires numeric operands"),
        )),
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
            } else if let Ok(number) = string.parse::<f64>() {
                number.is_finite().then_some(number)
            } else {
                None
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

fn literal_value(node: &ExprNode) -> Option<&Value> {
    match node {
        ExprNode::Literal(value) => Some(value),
        _ => None,
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
    Now,
    LParen,
    RParen,
    Comma,
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
    In,
    Between,
    Contains,
    StartsWith,
    EndsWith,
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
            b',' => {
                self.cursor += 1;
                Ok(Token::Comma)
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

        if self.cursor < self.bytes.len() && matches!(self.bytes[self.cursor], b'e' | b'E') {
            is_double = true;
            self.cursor += 1;
            if self.cursor < self.bytes.len() && matches!(self.bytes[self.cursor], b'+' | b'-') {
                self.cursor += 1;
            }
            let exponent_start = self.cursor;
            while self.cursor < self.bytes.len() && self.bytes[self.cursor].is_ascii_digit() {
                self.cursor += 1;
            }
            if exponent_start == self.cursor {
                return Err(FilterError::new("invalid exponent in numeric literal"));
            }
        }

        let token = &self.input[start..self.cursor];
        if is_double {
            let value = token
                .parse::<f64>()
                .map_err(|error| FilterError::new(format!("invalid number '{token}': {error}")))?;
            if !value.is_finite() {
                return Err(FilterError::new(format!("invalid number '{token}': overflow")));
            }
            Ok(Token::Double(value))
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
            "IN" => Ok(Token::In),
            "BETWEEN" => Ok(Token::Between),
            "CONTAINS" => Ok(Token::Contains),
            "STARTSWITH" => Ok(Token::StartsWith),
            "ENDSWITH" => Ok(Token::EndsWith),
            "TRUE" => Ok(Token::Boolean(true)),
            "FALSE" => Ok(Token::Boolean(false)),
            "NULL" => Ok(Token::Null),
            "NOW" => Ok(Token::Now),
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

        let negated_special = self.consume(TokenKind::Not);
        if self.consume(TokenKind::In) {
            return self.parse_in_list(left, negated_special);
        }
        if self.consume(TokenKind::Between) {
            return self.parse_between(left, negated_special);
        }
        if self.consume(TokenKind::Contains) {
            return self.parse_string_match(left, StringMatchOp::Contains, negated_special);
        }
        if self.consume(TokenKind::StartsWith) {
            return self.parse_string_match(left, StringMatchOp::StartsWith, negated_special);
        }
        if self.consume(TokenKind::EndsWith) {
            return self.parse_string_match(left, StringMatchOp::EndsWith, negated_special);
        }
        if negated_special {
            return Err(FilterError::new(
                "expected IN, BETWEEN, CONTAINS, STARTSWITH or ENDSWITH after NOT",
            ));
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
            Token::Now => Ok(ExprNode::Now),
            token => Err(FilterError::new(format!("unexpected token {token:?}"))),
        }
    }

    fn parse_constant_value(&mut self) -> Result<Value, FilterError> {
        match self.next() {
            Token::String(value) => Ok(Value::String(value)),
            Token::Long(value) => Ok(Value::Long(value)),
            Token::Double(value) => Ok(Value::Double(value)),
            Token::Boolean(value) => Ok(Value::Boolean(value)),
            Token::Null => Ok(Value::Null),
            token => Err(FilterError::new(format!("expected literal value, got {token:?}"))),
        }
    }

    fn parse_in_list(&mut self, left: ExprNode, negated: bool) -> Result<ExprNode, FilterError> {
        self.expect(TokenKind::LParen)?;
        let mut items = vec![self.parse_constant_value()?];
        while self.consume(TokenKind::Comma) {
            items.push(self.parse_constant_value()?);
        }
        self.expect(TokenKind::RParen)?;
        Ok(ExprNode::InList {
            expr: Box::new(left),
            items,
            negated,
        })
    }

    fn parse_between(&mut self, left: ExprNode, negated: bool) -> Result<ExprNode, FilterError> {
        let low = self.parse_value()?;
        self.expect(TokenKind::And)?;
        let high = self.parse_value()?;

        if let (Some(low), Some(high)) = (literal_value(&low), literal_value(&high)) {
            if matches!(low, Value::Null) || matches!(high, Value::Null) {
                return Err(FilterError::new("Illegal values of between, values can not be null"));
            }
            if let Some(false) =
                eval_compare_values(CompareOp::Lte, low, high).map_err(|error| FilterError::new(error.to_string()))?
            {
                return Err(FilterError::new(format!(
                    "Illegal values of between, left value({low}) must less than or equal to right value({high})"
                )));
            }
        }

        Ok(ExprNode::Between {
            expr: Box::new(left),
            low: Box::new(low),
            high: Box::new(high),
            negated,
        })
    }

    fn parse_string_match(
        &mut self,
        left: ExprNode,
        op: StringMatchOp,
        negated: bool,
    ) -> Result<ExprNode, FilterError> {
        let Token::String(search) = self.next() else {
            return Err(FilterError::new(
                "string match operators require a quoted string literal",
            ));
        };
        Ok(ExprNode::StringMatch {
            op,
            expr: Box::new(left),
            search,
            negated,
        })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    Null,
    LParen,
    RParen,
    Comma,
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
    In,
    Between,
    Contains,
    StartsWith,
    EndsWith,
}

impl TokenKind {
    fn matches(self, token: &Token) -> bool {
        matches!(
            (self, token),
            (TokenKind::Null, Token::Null)
                | (TokenKind::LParen, Token::LParen)
                | (TokenKind::RParen, Token::RParen)
                | (TokenKind::Comma, Token::Comma)
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
                | (TokenKind::In, Token::In)
                | (TokenKind::Between, Token::Between)
                | (TokenKind::Contains, Token::Contains)
                | (TokenKind::StartsWith, Token::StartsWith)
                | (TokenKind::EndsWith, Token::EndsWith)
        )
    }

    fn as_str(self) -> &'static str {
        match self {
            TokenKind::Null => "NULL",
            TokenKind::LParen => "(",
            TokenKind::RParen => ")",
            TokenKind::Comma => ",",
            TokenKind::Eq => "=",
            TokenKind::Ne => "!=",
            TokenKind::Gt => ">",
            TokenKind::Gte => ">=",
            TokenKind::Lt => "<",
            TokenKind::Lte => "<=",
            TokenKind::And => "AND",
            TokenKind::Or => "OR",
            TokenKind::Not => "NOT",
            TokenKind::Is => "IS",
            TokenKind::In => "IN",
            TokenKind::Between => "BETWEEN",
            TokenKind::Contains => "CONTAINS",
            TokenKind::StartsWith => "STARTSWITH",
            TokenKind::EndsWith => "ENDSWITH",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::compile_expression;
    use crate::expression::EmptyEvaluationContext;
    use crate::expression::EvaluationError;
    use crate::expression::MessageEvaluationContext;
    use crate::expression::Value;

    fn evaluate(expr: &str, pairs: &[(&str, &str)]) -> Result<Value, EvaluationError> {
        let expression = compile_expression(expr).expect("expression should compile");
        let mut context = MessageEvaluationContext::new();
        for (key, value) in pairs {
            context.put(*key, *value);
        }
        expression.evaluate(&context)
    }

    fn compile_error(expr: &str) -> String {
        match compile_expression(expr) {
            Ok(_) => panic!("expression should be rejected"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn compile_rejects_empty_expression() {
        let error = compile_error("   ");
        assert!(error.contains("empty SQL92 expression"));
    }

    #[test]
    fn evaluate_basic_comparison() {
        let value = evaluate(
            "color = 'blue' AND retries >= 3",
            &[("color", "blue"), ("retries", "3")],
        )
        .expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));
    }

    #[test]
    fn evaluate_boolean_cast_and_null_logic() {
        let expression = compile_expression("flag OR missing").expect("expression should compile");
        let mut context = MessageEvaluationContext::new();
        context.put("flag", "false");
        assert_eq!(expression.evaluate(&context).unwrap(), Value::Null);

        let expression = compile_expression("flag AND missing").expect("expression should compile");
        assert_eq!(expression.evaluate(&context).unwrap(), Value::Boolean(false));
    }

    #[test]
    fn evaluate_is_null() {
        let value = evaluate("missing IS NULL", &[]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));

        let value = evaluate("present IS NOT NULL", &[("present", "x")]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));
    }

    #[test]
    fn evaluate_in_and_not_in() {
        let value = evaluate("region IN ('hz', 'sh', 'bj')", &[("region", "sh")]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));

        let value =
            evaluate("region NOT IN ('hz', 'sh', 'bj')", &[("region", "cd")]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));

        let value = evaluate("region NOT IN ('hz', 'sh', 'bj')", &[]).expect("evaluation should succeed");
        assert_eq!(value, Value::Null);
    }

    #[test]
    fn evaluate_between_and_not_between() {
        let value = evaluate("price BETWEEN 10 AND 20", &[("price", "15")]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));

        let value = evaluate("price NOT BETWEEN 10 AND 20", &[("price", "25")]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));
    }

    #[test]
    fn evaluate_string_match_extensions() {
        let value = evaluate("name CONTAINS 'mq'", &[("name", "rocketmq-rust")]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));

        let value =
            evaluate("name STARTSWITH 'rocket'", &[("name", "rocketmq-rust")]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));

        let value = evaluate("name ENDSWITH 'rust'", &[("name", "rocketmq-rust")]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));

        let value =
            evaluate("name NOT CONTAINS 'java'", &[("name", "rocketmq-rust")]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));

        let value = evaluate("missing NOT CONTAINS 'java'", &[]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(false));
    }

    #[test]
    fn evaluate_now_keyword() {
        let before = rocketmq_common::TimeUtils::current_millis() as i64 - 1;
        let value = evaluate("ts <= NOW", &[("ts", &before.to_string())]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));
    }

    #[test]
    fn evaluate_boolean_literal_comparison() {
        let value =
            evaluate("a = TRUE OR b = FALSE", &[("a", "true"), ("b", "true")]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));
    }

    #[test]
    fn ordering_comparison_rejects_non_numeric_strings() {
        let error = evaluate("a BETWEEN 0 AND 100", &[("a", "abc")]).expect_err("evaluation should fail");
        assert!(matches!(error, EvaluationError::InvalidOperation(_)));
        assert!(error.to_string().contains("numeric operands"));
    }

    #[test]
    fn compile_rejects_illegal_between_bounds() {
        let error = compile_error("a BETWEEN 10 AND 0");
        assert!(error.contains("Illegal values of between"));
    }

    #[test]
    fn compile_rejects_non_string_contains_operand() {
        let error = compile_error("a CONTAINS 1");
        assert!(error.contains("string match operators require a quoted string literal"));
    }

    #[test]
    fn compile_rejects_invalid_number_exponent() {
        let error = compile_error("a = 1e");
        assert!(error.contains("invalid exponent"));
    }

    #[test]
    fn compile_supports_escaped_quotes() {
        let value = evaluate("name = 'rock''et'", &[("name", "rock'et")]).expect("evaluation should succeed");
        assert_eq!(value, Value::Boolean(true));
    }

    #[test]
    fn compile_rejects_trailing_tokens() {
        let error = compile_error("a = 1 2");
        assert!(error.contains("unexpected trailing token"));
    }

    #[test]
    fn evaluate_missing_value_as_null() {
        let expression = compile_expression("missing").expect("expression should compile");
        let value = expression
            .evaluate(&EmptyEvaluationContext)
            .expect("evaluation should succeed");
        assert_eq!(value, Value::Null);
    }
}
