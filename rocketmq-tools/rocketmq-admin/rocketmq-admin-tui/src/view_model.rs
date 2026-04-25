#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableViewModel {
    pub title: String,
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableViewport {
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub widths: Vec<u16>,
    pub hidden_left: bool,
    pub hidden_right: bool,
}

impl TableViewModel {
    const MIN_COLUMN_WIDTH: u16 = 8;
    const MAX_COLUMN_WIDTH: u16 = 32;

    pub fn viewport(&self, first_column: usize, available_width: u16) -> TableViewport {
        if self.headers.is_empty() {
            return TableViewport {
                headers: Vec::new(),
                rows: Vec::new(),
                widths: Vec::new(),
                hidden_left: false,
                hidden_right: false,
            };
        }

        let first_column = first_column.min(self.headers.len() - 1);
        let available_width = available_width.max(Self::MIN_COLUMN_WIDTH);
        let mut used_width = 0_u16;
        let mut columns = Vec::new();

        for column in first_column..self.headers.len() {
            let width = self.column_width(column).min(available_width);
            let separator_width = u16::from(!columns.is_empty());
            if !columns.is_empty() && used_width.saturating_add(separator_width).saturating_add(width) > available_width
            {
                break;
            }
            used_width = used_width.saturating_add(separator_width).saturating_add(width);
            columns.push((column, width));
        }

        let headers = columns
            .iter()
            .map(|(column, _)| self.headers[*column].clone())
            .collect::<Vec<_>>();
        let rows = self
            .rows
            .iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|(column, _)| row.get(*column).cloned().unwrap_or_default())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let hidden_right = columns
            .last()
            .map(|(column, _)| column + 1 < self.headers.len())
            .unwrap_or(false);

        TableViewport {
            headers,
            rows,
            widths: columns.into_iter().map(|(_, width)| width).collect(),
            hidden_left: first_column > 0,
            hidden_right,
        }
    }

    fn column_width(&self, column: usize) -> u16 {
        let header_width = self
            .headers
            .get(column)
            .map(|header| header.chars().count())
            .unwrap_or_default();
        let cell_width = self
            .rows
            .iter()
            .filter_map(|row| row.get(column))
            .map(|cell| cell.chars().count())
            .max()
            .unwrap_or_default();
        let width = header_width.max(cell_width).saturating_add(2);
        width.clamp(Self::MIN_COLUMN_WIDTH as usize, Self::MAX_COLUMN_WIDTH as usize) as u16
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValueViewModel {
    pub title: String,
    pub rows: Vec<(String, String)>,
}

impl KeyValueViewModel {
    pub fn sorted(title: impl Into<String>, mut rows: Vec<(String, String)>) -> Self {
        rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        Self {
            title: title.into(),
            rows,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationSummaryViewModel {
    pub title: String,
    pub success_count: usize,
    pub failure_count: usize,
    pub targets: Vec<String>,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandResultViewModel {
    Table(TableViewModel),
    KeyValue(KeyValueViewModel),
    Json { title: String, body: String },
    Text { title: String, body: String },
    OperationSummary(OperationSummaryViewModel),
}

mod conversions;

#[cfg(test)]
mod tests;
