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

use cheetah_string::CheetahString;
use rocketmq_common::common::filter::expression_type::ExpressionType;

/// Selects messages at the server side based on filter expressions.
///
/// Supports two types of filtering:
/// - **TAG**: Simple tag-based filtering using `||` operator for multiple tags
/// - **SQL92**: Advanced SQL-like filtering with conditions on message properties
///
/// # Examples
///
/// ```rust
/// use rocketmq_client::consumer::message_selector::MessageSelector;
///
/// // Select by tag
/// let selector = MessageSelector::by_tag("TagA");
///
/// // Select by SQL92 expression
/// let selector = MessageSelector::by_sql("a > 10 AND b = 'value'");
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct MessageSelector {
    /// Expression type: "TAG" or "SQL92"
    expression_type: CheetahString,
    /// Filter expression content
    expression: CheetahString,
}

impl MessageSelector {
    /// Creates a new message selector with the specified type and expression.
    fn new(expression_type: impl Into<CheetahString>, expression: impl Into<CheetahString>) -> Self {
        Self {
            expression_type: expression_type.into(),
            expression: expression.into(),
        }
    }

    /// Creates a selector using SQL92 syntax for advanced filtering.
    ///
    /// SQL92 filtering supports:
    /// - **Keywords**: `AND`, `OR`, `NOT`, `BETWEEN`, `IN`, `TRUE`, `FALSE`, `IS`, `NULL`
    /// - **Data types**: Boolean, String (quoted), Decimal, Float
    /// - **Operators**: `>`, `>=`, `<`, `<=`, `=`
    /// - **Special operations**:
    ///   - `BETWEEN A AND B` (equivalent to `>= A AND <= B`)
    ///   - `IN ('a', 'b')` (equivalent to `= 'a' OR = 'b'`)
    ///   - `IS NULL`, `IS NOT NULL`
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL92 filter expression. If `None`, empty, or `"*"`, selects all messages.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rocketmq_client::consumer::message_selector::MessageSelector;
    ///
    /// let selector =
    ///     MessageSelector::by_sql("(price > 100 AND category = 'electronics') OR discount = true");
    /// ```
    pub fn by_sql(sql: impl Into<CheetahString>) -> Self {
        Self::new(ExpressionType::SQL92, sql)
    }

    /// Creates a selector using tag-based filtering.
    ///
    /// Tag filtering supports multiple tags separated by `||` (OR operation).
    ///
    /// # Arguments
    ///
    /// * `tag` - Tag expression. If `None`, empty, or `"*"`, selects all messages.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rocketmq_client::consumer::message_selector::MessageSelector;
    ///
    /// // Single tag
    /// let selector = MessageSelector::by_tag("TagA");
    ///
    /// // Multiple tags (OR operation)
    /// let selector = MessageSelector::by_tag("TagA || TagB || TagC");
    ///
    /// // Subscribe all
    /// let selector = MessageSelector::by_tag("*");
    /// ```
    pub fn by_tag(tag: impl Into<CheetahString>) -> Self {
        Self::new(ExpressionType::TAG, tag)
    }

    /// Returns the expression type ("TAG" or "SQL92").
    pub fn get_expression_type(&self) -> &CheetahString {
        &self.expression_type
    }

    /// Returns the filter expression content.
    pub fn get_expression(&self) -> &CheetahString {
        &self.expression
    }
}
