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

//! Prepared command results and their scroll state.
//!
//! A result is measured once when it arrives: column widths, numeric columns, and
//! line breaks are cached so a frame only touches the rows that are on screen.

use crate::text::display_width;
use crate::text::wrap;
use crate::view_model::CommandResultViewModel;

const MIN_COLUMN_WIDTH: usize = 3;
const MAX_COLUMN_WIDTH: usize = 48;
/// Blank cells between two adjacent grid columns.
pub(crate) const COLUMN_GAP: u16 = 2;
/// Narrowest slice of a clipped trailing column that is still worth showing.
const PARTIAL_COLUMN_MIN: u16 = 6;
/// Cells panned per horizontal step when a line document does not wrap.
const PAN_STEP: usize = 8;
const TAB_STOP: &str = "    ";
/// Leading marks of the target and error lines of an operation summary.
pub(crate) const SUCCESS_MARK: char = '✓';
pub(crate) const FAILURE_MARK: char = '×';

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultTone {
    Normal,
    Failure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineKind {
    Plain,
    /// A line of JSON or of a Rust debug representation, colored token by token.
    Structured,
    Heading,
    Success,
    Failure,
    /// Success and failure totals of an operation summary.
    Summary {
        succeeded: usize,
        failed: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocLine {
    pub kind: LineKind,
    pub text: String,
}

/// Visible grid columns for one horizontal scroll position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnWindow {
    /// Visible columns as `(column index, rendered width)`.
    pub columns: Vec<(usize, u16)>,
    pub hidden_left: bool,
    pub hidden_right: bool,
}

/// Tabular result with cached column measurements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grid {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    widths: Vec<u16>,
    numeric: Vec<bool>,
}

impl Grid {
    fn new(headers: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        let mut widths = headers.iter().map(|header| display_width(header)).collect::<Vec<_>>();
        let mut numeric = vec![true; headers.len()];
        let mut populated = vec![false; headers.len()];
        for row in &rows {
            for (column, cell) in row.iter().enumerate().take(headers.len()) {
                widths[column] = widths[column].max(display_width(cell));
                let value = cell.trim();
                if !value.is_empty() && value != "-" {
                    populated[column] = true;
                    numeric[column] &= value.parse::<f64>().is_ok();
                }
            }
        }
        Self {
            headers,
            rows,
            widths: widths
                .into_iter()
                .map(|width| width.clamp(MIN_COLUMN_WIDTH, MAX_COLUMN_WIDTH) as u16)
                .collect(),
            numeric: numeric
                .into_iter()
                .zip(populated)
                .map(|(numeric, populated)| numeric && populated)
                .collect(),
        }
    }

    pub fn headers(&self) -> &[String] {
        &self.headers
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Returns a cell, or an empty string for a short row.
    pub fn cell(&self, row: usize, column: usize) -> &str {
        self.rows
            .get(row)
            .and_then(|cells| cells.get(column))
            .map_or("", String::as_str)
    }

    /// Returns whether every populated cell of `column` is a number.
    pub fn is_numeric(&self, column: usize) -> bool {
        self.numeric.get(column).copied().unwrap_or(false)
    }

    /// Selects the columns that fit `available` cells starting at column `first`.
    ///
    /// Columns keep their measured width. A trailing column that does not fit is
    /// shown clipped when enough room is left, which signals more content to the
    /// right; a final text column absorbs any spare width.
    pub fn column_window(&self, first: usize, available: u16) -> ColumnWindow {
        if self.headers.is_empty() || available == 0 {
            return ColumnWindow {
                columns: Vec::new(),
                hidden_left: false,
                hidden_right: false,
            };
        }

        let first = first.min(self.headers.len() - 1);
        let mut columns: Vec<(usize, u16)> = Vec::new();
        let mut used = 0_u16;
        let mut clipped = false;
        for column in first..self.headers.len() {
            let natural = self.widths[column];
            if columns.is_empty() {
                let width = natural.min(available);
                clipped = width < natural;
                columns.push((column, width));
                used = width;
                continue;
            }
            let remaining = available.saturating_sub(used.saturating_add(COLUMN_GAP));
            if remaining >= natural {
                columns.push((column, natural));
                used = used.saturating_add(COLUMN_GAP).saturating_add(natural);
                continue;
            }
            if remaining >= PARTIAL_COLUMN_MIN {
                columns.push((column, remaining));
                used = available;
                clipped = true;
            }
            break;
        }

        let last = columns.last().map_or(first, |(column, _)| *column);
        let hidden_right = clipped || last + 1 < self.headers.len();
        if !hidden_right && !self.is_numeric(last) {
            if let Some((_, width)) = columns.last_mut() {
                *width = width.saturating_add(available.saturating_sub(used));
            }
        }

        ColumnWindow {
            columns,
            hidden_left: first > 0,
            hidden_right,
        }
    }

    fn fields(&self, row: usize) -> Option<Vec<(String, String)>> {
        let cells = self.rows.get(row)?;
        Some(
            self.headers
                .iter()
                .enumerate()
                .map(|(column, header)| (header.clone(), cells.get(column).cloned().unwrap_or_default()))
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultBody {
    Grid(Grid),
    Lines(Vec<DocLine>),
}

/// One rendered row of a line document: a byte range of a logical line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowSpan {
    pub line: usize,
    pub start: usize,
    pub end: usize,
    /// Whether this is the first row of its logical line.
    pub first: bool,
}

#[derive(Debug)]
struct WrapLayout {
    width: u16,
    rows: Vec<RowSpan>,
}

/// A command result prepared for rendering, together with its viewport.
#[derive(Debug)]
pub struct ResultView {
    title: String,
    tone: ResultTone,
    body: ResultBody,
    /// First visible row: a grid row or a rendered line row.
    top: usize,
    /// Selected grid row.
    cursor: usize,
    /// First visible grid column, or the pan offset in cells of an unwrapped document.
    column: usize,
    wrap: bool,
    wrapped: Option<WrapLayout>,
    /// Logical line to keep at the top of the viewport across the next re-wrap.
    anchor_line: Option<usize>,
    max_line_width: usize,
}

impl ResultView {
    pub fn new(model: CommandResultViewModel, tone: ResultTone) -> Self {
        let title = model.title().to_string();
        let mut wrap = true;
        let body = match model {
            CommandResultViewModel::Table(table) if !table.headers.is_empty() => {
                ResultBody::Grid(Grid::new(table.headers, table.rows))
            }
            CommandResultViewModel::Table(table) => ResultBody::Lines(
                table
                    .rows
                    .iter()
                    .map(|row| DocLine {
                        kind: LineKind::Plain,
                        text: sanitize(&row.join(" | ")),
                    })
                    .collect(),
            ),
            CommandResultViewModel::KeyValue(key_values) => ResultBody::Grid(Grid::new(
                vec!["Key".to_string(), "Value".to_string()],
                key_values
                    .rows
                    .into_iter()
                    .map(|(key, value)| vec![key, value])
                    .collect(),
            )),
            CommandResultViewModel::Json { body, .. } => {
                wrap = false;
                ResultBody::Lines(lines_of(&body, LineKind::Structured))
            }
            CommandResultViewModel::Text { body, .. } => {
                // A failure is drawn in one color; only a returned value is worth parsing.
                let kind = if tone == ResultTone::Normal && is_debug_dump(&body) {
                    LineKind::Structured
                } else {
                    LineKind::Plain
                };
                ResultBody::Lines(lines_of(&body, kind))
            }
            CommandResultViewModel::OperationSummary(summary) => {
                let mut lines = vec![DocLine {
                    kind: LineKind::Summary {
                        succeeded: summary.success_count,
                        failed: summary.failure_count,
                    },
                    text: format!("{} succeeded, {} failed", summary.success_count, summary.failure_count),
                }];
                append_section(&mut lines, "Targets", LineKind::Success, SUCCESS_MARK, &summary.targets);
                append_section(&mut lines, "Errors", LineKind::Failure, FAILURE_MARK, &summary.errors);
                ResultBody::Lines(lines)
            }
        };
        let max_line_width = match &body {
            ResultBody::Lines(lines) => lines.iter().map(|line| display_width(&line.text)).max().unwrap_or(0),
            ResultBody::Grid(_) => 0,
        };

        Self {
            title,
            tone,
            body,
            top: 0,
            cursor: 0,
            column: 0,
            wrap,
            wrapped: None,
            anchor_line: None,
            max_line_width,
        }
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn tone(&self) -> ResultTone {
        self.tone
    }

    pub fn body(&self) -> &ResultBody {
        &self.body
    }

    pub fn grid(&self) -> Option<&Grid> {
        match &self.body {
            ResultBody::Grid(grid) => Some(grid),
            ResultBody::Lines(_) => None,
        }
    }

    pub fn top(&self) -> usize {
        self.top
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn column(&self) -> usize {
        self.column
    }

    /// Returns whether a line document currently wraps long lines.
    pub fn wraps(&self) -> bool {
        self.wrap && matches!(self.body, ResultBody::Lines(_))
    }

    /// Returns the number of scrollable rows: grid rows or rendered line rows.
    pub fn row_count(&self) -> usize {
        match (&self.body, &self.wrapped) {
            (ResultBody::Grid(grid), _) => grid.row_count(),
            (ResultBody::Lines(_), Some(layout)) => layout.rows.len(),
            (ResultBody::Lines(lines), None) => lines.len(),
        }
    }

    /// Returns the rendered row at `index` of a line document.
    pub fn row(&self, index: usize) -> Option<RowSpan> {
        match (&self.body, &self.wrapped) {
            (ResultBody::Grid(_), _) => None,
            (ResultBody::Lines(_), Some(layout)) => layout.rows.get(index).copied(),
            (ResultBody::Lines(lines), None) => lines.get(index).map(|line| RowSpan {
                line: index,
                start: 0,
                end: line.text.len(),
                first: true,
            }),
        }
    }

    pub fn line(&self, index: usize) -> Option<&DocLine> {
        match &self.body {
            ResultBody::Lines(lines) => lines.get(index),
            ResultBody::Grid(_) => None,
        }
    }

    /// Returns the header and cell pairs of the selected grid row.
    pub fn selected_fields(&self) -> Option<Vec<(String, String)>> {
        self.grid()?.fields(self.cursor)
    }

    /// Fits the viewport to a content area of `width` cells by `rows` rows.
    ///
    /// Re-wraps a line document when the width changed and clamps every offset, so
    /// scrolling requests never have to know the size of the pane.
    pub fn sync(&mut self, width: u16, rows: usize) {
        if matches!(self.body, ResultBody::Lines(_)) {
            if !self.wrap {
                self.wrapped = None;
            } else if self.wrapped.as_ref().is_none_or(|layout| layout.width != width) {
                self.rewrap(width);
            }
        }

        let total = self.row_count();
        let max_top = total.saturating_sub(rows.max(1));
        match &self.body {
            ResultBody::Grid(grid) => {
                self.cursor = self.cursor.min(total.saturating_sub(1));
                if rows > 0 {
                    if self.cursor < self.top {
                        self.top = self.cursor;
                    } else if self.cursor >= self.top.saturating_add(rows) {
                        self.top = self.cursor + 1 - rows;
                    }
                }
                self.top = self.top.min(max_top);
                self.column = self.column.min(grid.headers.len().saturating_sub(1));
            }
            ResultBody::Lines(_) => {
                self.top = self.top.min(max_top);
                self.column = if self.wrap {
                    0
                } else {
                    self.column.min(self.max_line_width.saturating_sub(usize::from(width)))
                };
            }
        }
    }

    /// Moves the grid selection, or scrolls a line document, by `delta` rows.
    pub fn move_rows(&mut self, delta: isize) {
        let last = self.row_count().saturating_sub(1);
        match self.body {
            ResultBody::Grid(_) => self.cursor = offset(self.cursor, delta).min(last),
            ResultBody::Lines(_) => self.top = offset(self.top, delta).min(last),
        }
    }

    /// Scrolls the viewport by `delta` rows and keeps the grid selection inside it.
    pub fn scroll_viewport(&mut self, delta: isize, rows: usize) {
        let rows = rows.max(1);
        let total = self.row_count();
        self.top = offset(self.top, delta).min(total.saturating_sub(rows));
        if matches!(self.body, ResultBody::Grid(_)) {
            let bottom = (self.top + rows - 1).min(total.saturating_sub(1));
            self.cursor = self.cursor.clamp(self.top.min(bottom), bottom);
        }
    }

    pub fn move_to_start(&mut self) {
        self.top = 0;
        self.cursor = 0;
    }

    pub fn move_to_end(&mut self) {
        let last = self.row_count().saturating_sub(1);
        self.cursor = last;
        self.top = last;
    }

    /// Scrolls grid columns, or pans an unwrapped line document, by `delta` steps.
    pub fn move_columns(&mut self, delta: isize) {
        match &self.body {
            ResultBody::Grid(grid) => {
                self.column = offset(self.column, delta).min(grid.headers.len().saturating_sub(1));
            }
            ResultBody::Lines(_) if !self.wrap => {
                self.column = offset(self.column, delta.saturating_mul(PAN_STEP as isize)).min(self.max_line_width);
            }
            ResultBody::Lines(_) => {}
        }
    }

    /// Selects a grid row, returning whether it was already selected.
    pub fn select_row(&mut self, row: usize) -> bool {
        if !matches!(self.body, ResultBody::Grid(_)) {
            return false;
        }
        let row = row.min(self.row_count().saturating_sub(1));
        let already_selected = self.cursor == row;
        self.cursor = row;
        already_selected
    }

    /// Toggles line wrapping, returning the new setting, or `None` for a grid.
    pub fn toggle_wrap(&mut self) -> Option<bool> {
        if !matches!(self.body, ResultBody::Lines(_)) {
            return None;
        }
        if self.wrap {
            if let Some(row) = self.wrapped.as_ref().and_then(|layout| layout.rows.get(self.top)) {
                self.top = row.line;
            }
            self.wrapped = None;
        } else {
            self.anchor_line = Some(self.top);
            self.column = 0;
        }
        self.wrap = !self.wrap;
        Some(self.wrap)
    }

    fn rewrap(&mut self, width: u16) {
        let ResultBody::Lines(lines) = &self.body else {
            return;
        };
        let anchor = self.anchor_line.take().or_else(|| {
            self.wrapped
                .as_ref()
                .and_then(|layout| layout.rows.get(self.top))
                .map(|row| row.line)
        });
        let rows = wrap_lines(lines, width);
        if let Some(line) = anchor {
            self.top = rows.partition_point(|row| row.line < line);
        }
        self.wrapped = Some(WrapLayout { width, rows });
    }
}

fn offset(value: usize, delta: isize) -> usize {
    if delta.is_negative() {
        value.saturating_sub(delta.unsigned_abs())
    } else {
        value.saturating_add(delta.unsigned_abs())
    }
}

/// Returns whether `body` is the multi-line debug representation of a value.
///
/// Such a document opens a struct, a list, or a tuple on its first line. Prose and
/// single-line values stay plain text.
fn is_debug_dump(body: &str) -> bool {
    let mut lines = body.lines().filter(|line| !line.trim().is_empty());
    let opens_a_scope = lines
        .next()
        .is_some_and(|first| first.trim_end().ends_with(['{', '[', '(']));
    opens_a_scope && lines.next().is_some()
}

fn lines_of(body: &str, kind: LineKind) -> Vec<DocLine> {
    body.lines()
        .map(|line| DocLine {
            kind,
            text: sanitize(line),
        })
        .collect()
}

fn append_section(lines: &mut Vec<DocLine>, heading: &str, kind: LineKind, mark: char, entries: &[String]) {
    if entries.is_empty() {
        return;
    }
    lines.push(DocLine {
        kind: LineKind::Plain,
        text: String::new(),
    });
    lines.push(DocLine {
        kind: LineKind::Heading,
        text: format!("{heading} ({})", entries.len()),
    });
    // The mark is part of the text so wrapping accounts for its width.
    lines.extend(entries.iter().map(|entry| DocLine {
        kind,
        text: format!("{mark} {}", sanitize(entry)),
    }));
}

/// Expands tabs and drops control characters that a terminal cell cannot show.
fn sanitize(line: &str) -> String {
    if !line.contains(|character: char| character.is_control()) {
        return line.to_string();
    }
    let mut sanitized = String::with_capacity(line.len());
    for character in line.chars() {
        if character == '\t' {
            sanitized.push_str(TAB_STOP);
        } else if !character.is_control() {
            sanitized.push(character);
        }
    }
    sanitized
}

fn wrap_lines(lines: &[DocLine], width: u16) -> Vec<RowSpan> {
    let mut rows = Vec::with_capacity(lines.len());
    for (line, entry) in lines.iter().enumerate() {
        let mut first = true;
        wrap(&entry.text, usize::from(width), |start, end| {
            rows.push(RowSpan {
                line,
                start,
                end,
                first,
            });
            first = false;
        });
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::view_model::KeyValueViewModel;
    use crate::view_model::OperationSummaryViewModel;
    use crate::view_model::TableViewModel;

    fn table(headers: &[&str], rows: &[&[&str]]) -> ResultView {
        ResultView::new(
            CommandResultViewModel::Table(TableViewModel {
                title: "table".to_string(),
                headers: headers.iter().map(ToString::to_string).collect(),
                rows: rows
                    .iter()
                    .map(|row| row.iter().map(ToString::to_string).collect())
                    .collect(),
            }),
            ResultTone::Normal,
        )
    }

    fn text(body: &str) -> ResultView {
        ResultView::new(
            CommandResultViewModel::Text {
                title: "text".to_string(),
                body: body.to_string(),
            },
            ResultTone::Normal,
        )
    }

    fn row_texts(view: &ResultView) -> Vec<String> {
        (0..view.row_count())
            .filter_map(|index| view.row(index))
            .map(|row| view.line(row.line).unwrap().text[row.start..row.end].to_string())
            .collect()
    }

    fn line_kinds(view: &ResultView) -> Vec<LineKind> {
        let ResultBody::Lines(lines) = view.body() else {
            panic!("a text result is a line document");
        };
        lines.iter().map(|line| line.kind).collect()
    }

    #[test]
    fn only_a_returned_debug_representation_is_colored_as_structured_text() {
        let dump = format!("{:#?}", Some(("TopicA", vec![0, 1])));
        assert!(dump.starts_with("Some(\n"));

        for structured in [dump.clone(), format!("{:#?}", vec!["a", "b"])] {
            assert!(
                line_kinds(&text(&structured))
                    .iter()
                    .all(|kind| *kind == LineKind::Structured),
                "{structured:?}"
            );
        }

        for plain in [
            "None",
            "Topic created on 2 brokers.\nNothing else changed.",
            "",
            "TopicConfig {",
        ] {
            assert!(
                line_kinds(&text(plain)).iter().all(|kind| *kind == LineKind::Plain),
                "{plain:?}"
            );
        }

        let failure = ResultView::new(
            CommandResultViewModel::Text {
                title: "failure".to_string(),
                body: dump,
            },
            ResultTone::Failure,
        );
        assert!(line_kinds(&failure).iter().all(|kind| *kind == LineKind::Plain));
    }

    #[test]
    fn result_view_models_convert_to_grids_and_documents() {
        let view = table(&["a", "b"], &[&["1", "2"]]);
        let grid = view.grid().unwrap();
        assert_eq!(view.title(), "table");
        assert_eq!(grid.headers(), ["a", "b"]);
        assert_eq!(grid.cell(0, 1), "2");
        assert_eq!(grid.cell(0, 9), "");
        assert_eq!(grid.cell(9, 0), "");

        let key_value = ResultView::new(
            CommandResultViewModel::KeyValue(KeyValueViewModel {
                title: "kv".to_string(),
                rows: vec![("key".to_string(), "value".to_string())],
            }),
            ResultTone::Normal,
        );
        let grid = key_value.grid().unwrap();
        assert_eq!(grid.headers(), ["Key", "Value"]);
        assert_eq!((grid.cell(0, 0), grid.cell(0, 1)), ("key", "value"));

        let summary = ResultView::new(
            CommandResultViewModel::OperationSummary(OperationSummaryViewModel {
                title: "summary".to_string(),
                success_count: 1,
                failure_count: 1,
                targets: vec!["topic-a".to_string()],
                errors: vec!["failed".to_string()],
            }),
            ResultTone::Normal,
        );
        let ResultBody::Lines(lines) = summary.body() else {
            panic!("an operation summary is a line document");
        };
        assert_eq!(
            lines[0].kind,
            LineKind::Summary {
                succeeded: 1,
                failed: 1
            }
        );
        assert!(lines
            .iter()
            .any(|line| line.kind == LineKind::Success && line.text == "✓ topic-a"));
        assert!(lines
            .iter()
            .any(|line| line.kind == LineKind::Failure && line.text == "× failed"));
        assert!(lines
            .iter()
            .any(|line| line.kind == LineKind::Heading && line.text == "Errors (1)"));
    }

    #[test]
    fn a_table_without_headers_falls_back_to_a_line_document() {
        let view = table(&[], &[&["1", "2"], &["3", "4"]]);

        assert!(view.grid().is_none());
        assert_eq!(view.line(0).unwrap().text, "1 | 2");
        assert_eq!(view.row_count(), 2);
    }

    #[test]
    fn column_window_slices_columns_by_horizontal_scroll() {
        let view = table(
            &["cluster", "broker", "addr", "status"],
            &[&["cluster-a", "broker-a", "127.0.0.1:10911", "online"]],
        );
        let grid = view.grid().unwrap();

        // Width 12 holds `broker` (8 cells) but leaves too little room to clip `addr`.
        let window = grid.column_window(1, 12);
        assert!(window.hidden_left);
        assert!(window.hidden_right);
        assert_eq!(window.columns, vec![(1, 8)]);

        // With room to spare, the next column is shown clipped.
        let window = grid.column_window(1, 18);
        assert_eq!(window.columns, vec![(1, 8), (2, 8)]);
        assert!(window.hidden_right);

        let window = grid.column_window(0, 80);
        assert_eq!(window.columns.len(), 4);
        assert!(!window.hidden_left);
        assert!(!window.hidden_right);
        assert_eq!(
            window.columns.iter().map(|(_, width)| width).sum::<u16>(),
            80 - 3 * COLUMN_GAP
        );
    }

    #[test]
    fn column_window_clamps_out_of_range_scroll_to_last_column() {
        let view = table(&["a", "b"], &[&["1", "2"]]);
        let window = view.grid().unwrap().column_window(99, 80);

        assert!(window.hidden_left);
        assert!(!window.hidden_right);
        assert_eq!(window.columns.len(), 1);
        assert_eq!(window.columns[0].0, 1);
        assert!(view.grid().unwrap().column_window(0, 0).columns.is_empty());
    }

    #[test]
    fn numeric_columns_are_detected_and_never_stretched() {
        let view = table(
            &["name", "offset", "note"],
            &[&["a", "12", "x"], &["b", "-", ""], &["c", "3.5", "y"]],
        );
        let grid = view.grid().unwrap();

        assert!(!grid.is_numeric(0));
        assert!(grid.is_numeric(1));
        assert!(!grid.is_numeric(2));
        assert!(!grid.is_numeric(9));

        let numbers = table(&["name", "offset"], &[&["a", "12"]]);
        let window = numbers.grid().unwrap().column_window(0, 40);
        assert_eq!(window.columns, vec![(0, 4), (1, 6)]);
    }

    #[test]
    fn grid_viewport_follows_the_selection_and_clamps_offsets() {
        let rows = (0..20).map(|index| vec![index.to_string()]).collect::<Vec<_>>();
        let mut view = ResultView::new(
            CommandResultViewModel::Table(TableViewModel {
                title: "table".to_string(),
                headers: vec!["n".to_string()],
                rows,
            }),
            ResultTone::Normal,
        );

        view.move_rows(7);
        view.sync(40, 5);
        assert_eq!((view.cursor(), view.top()), (7, 3));

        view.move_rows(-6);
        view.sync(40, 5);
        assert_eq!((view.cursor(), view.top()), (1, 1));

        view.move_to_end();
        view.sync(40, 5);
        assert_eq!((view.cursor(), view.top()), (19, 15));

        view.scroll_viewport(-10, 5);
        assert_eq!((view.cursor(), view.top()), (9, 5));

        view.move_columns(5);
        view.sync(40, 5);
        assert_eq!(view.column(), 0);

        assert!(!view.select_row(12));
        assert!(view.select_row(12));
        assert_eq!(
            view.selected_fields().unwrap(),
            vec![("n".to_string(), "12".to_string())]
        );
    }

    #[test]
    fn wrapping_prefers_whitespace_and_splits_long_words() {
        let mut view = text("alpha beta gamma\n\nabcdefghij");
        view.sync(8, 10);

        assert_eq!(row_texts(&view), ["alpha ", "beta ", "gamma", "", "abcdefgh", "ij"]);
        assert!(view.row(0).unwrap().first);
        assert!(!view.row(1).unwrap().first);
        assert_eq!(view.row(3).unwrap().line, 1);
    }

    #[test]
    fn wrapping_handles_wide_characters_and_narrow_panes() {
        let mut view = text("主题主题主");
        view.sync(5, 10);
        assert_eq!(row_texts(&view), ["主题", "主题", "主"]);

        // A character wider than the pane still makes progress.
        view.sync(1, 10);
        assert_eq!(view.row_count(), 5);
    }

    #[test]
    fn toggling_wrap_keeps_the_same_logical_line_on_top() {
        let mut view = text("one two three four\nsecond\nthird line here");
        view.sync(6, 2);
        let third = (0..view.row_count())
            .find(|index| view.row(*index).unwrap().line == 2)
            .unwrap();
        view.move_rows(third as isize);
        view.sync(6, 2);
        assert_eq!(view.row(view.top()).unwrap().line, 2);

        assert_eq!(view.toggle_wrap(), Some(false));
        view.sync(6, 2);
        assert_eq!(view.top(), 1, "an unwrapped document clamps to its last full page");
        assert_eq!(view.row_count(), 3);

        view.move_rows(1);
        assert_eq!(view.toggle_wrap(), Some(true));
        view.sync(6, 2);
        assert_eq!(view.row(view.top()).unwrap().line, 2);

        let mut grid = table(&["a"], &[&["1"]]);
        assert_eq!(grid.toggle_wrap(), None);
    }

    #[test]
    fn unwrapped_documents_pan_within_the_longest_line() {
        let mut view = ResultView::new(
            CommandResultViewModel::Json {
                title: "json".to_string(),
                body: "{\n  \"key\": \"a long value that needs panning\"\n}".to_string(),
            },
            ResultTone::Normal,
        );
        assert!(!view.wraps());

        view.move_columns(100);
        view.sync(20, 5);
        assert_eq!(view.column(), view.max_line_width - 20);

        view.move_columns(-100);
        view.sync(20, 5);
        assert_eq!(view.column(), 0);
    }

    #[test]
    fn control_characters_are_removed_from_documents() {
        let view = text("a\tb\u{7}c\r\nnext");

        assert_eq!(view.line(0).unwrap().text, "a    bc");
        assert_eq!(view.line(1).unwrap().text, "next");
    }
}
