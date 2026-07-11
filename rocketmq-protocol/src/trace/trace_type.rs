use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TraceType {
    #[default]
    Pub,
    SubBefore,
    SubAfter,
    EndTransaction,
    Recall,
}

impl TraceType {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "Pub" => Some(Self::Pub),
            "SubBefore" => Some(Self::SubBefore),
            "SubAfter" => Some(Self::SubAfter),
            "EndTransaction" => Some(Self::EndTransaction),
            "Recall" => Some(Self::Recall),
            _ => None,
        }
    }
}

impl Display for TraceType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Pub => "Pub",
            Self::SubBefore => "SubBefore",
            Self::SubAfter => "SubAfter",
            Self::EndTransaction => "EndTransaction",
            Self::Recall => "Recall",
        })
    }
}
