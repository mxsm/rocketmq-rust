use crate::protocol::body::broker_item::BrokerStatsItem;

#[derive(Debug, Clone)]
/// Represents broker statistics over different time periods (minute, hour, day)
pub struct BrokerStatsData {
    /// Statistics for the last minute
    stats_minute: BrokerStatsItem,
    /// Statistics for the last hour
    stats_hour: BrokerStatsItem,
    /// Statistics for the last day
    stats_day: BrokerStatsItem,
}

impl BrokerStatsData {
    pub fn new(stats_minute: BrokerStatsItem, stats_hour: BrokerStatsItem, stats_day: BrokerStatsItem) -> Self {
        Self {
            stats_minute,
            stats_hour,
            stats_day,
        }
    }

    pub fn get_stats_minute(&self) -> &BrokerStatsItem {
        &self.stats_minute
    }

    pub fn set_stats_minute(&mut self, stats_minute: BrokerStatsItem) {
        self.stats_minute = stats_minute;
    }

    pub fn get_stats_hour(&self) -> &BrokerStatsItem {
        &self.stats_hour
    }

    pub fn set_stats_hour(&mut self, stats_hour: BrokerStatsItem) {
        self.stats_hour = stats_hour;
    }

    pub fn get_stats_day(&self) -> &BrokerStatsItem {
        &self.stats_day
    }

    pub fn set_stats_day(&mut self, stats_day: BrokerStatsItem) {
        self.stats_day = stats_day;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_values() {
        let stats_minute = BrokerStatsItem::new(100, 12.5, 8.3);
        let stats_hour = BrokerStatsItem::new(500, 15.0, 9.0);
        let stats_day = BrokerStatsItem::new(1000, 20.0, 10.0);

        let broker_stats = BrokerStatsData::new(stats_minute.clone(), stats_hour.clone(), stats_day.clone());

        assert_eq!(broker_stats.get_stats_minute().get_sum(), 100);
        assert_eq!(broker_stats.get_stats_hour().get_sum(), 500);
        assert_eq!(broker_stats.get_stats_day().get_sum(), 1000);

        assert_eq!(broker_stats.get_stats_minute().get_tps(), 12.5);
        assert_eq!(broker_stats.get_stats_hour().get_tps(), 15.0);
        assert_eq!(broker_stats.get_stats_day().get_tps(), 20.0);
    }

    #[test]
    fn test_set_stats_minute() {
        let stats_minute = BrokerStatsItem::new(100, 12.5, 8.3);
        let stats_hour = BrokerStatsItem::new(500, 15.0, 9.0);
        let stats_day = BrokerStatsItem::new(1000, 20.0, 10.0);

        let mut broker_stats = BrokerStatsData::new(stats_minute, stats_hour, stats_day);

        let new_stats_minute = BrokerStatsItem::new(200, 25.0, 12.0);
        broker_stats.set_stats_minute(new_stats_minute.clone());

        assert_eq!(broker_stats.get_stats_minute().get_sum(), 200);
        assert_eq!(broker_stats.get_stats_minute().get_tps(), 25.0);
        assert_eq!(broker_stats.get_stats_minute().get_avgpt(), 12.0);
    }

    #[test]
    fn test_set_stats_hour() {
        let stats_minute = BrokerStatsItem::new(100, 12.5, 8.3);
        let stats_hour = BrokerStatsItem::new(500, 15.0, 9.0);
        let stats_day = BrokerStatsItem::new(1000, 20.0, 10.0);

        let mut broker_stats = BrokerStatsData::new(stats_minute, stats_hour, stats_day);

        let new_stats_hour = BrokerStatsItem::new(600, 18.0, 10.0);
        broker_stats.set_stats_hour(new_stats_hour.clone());

        assert_eq!(broker_stats.get_stats_hour().get_sum(), 600);
        assert_eq!(broker_stats.get_stats_hour().get_tps(), 18.0);
        assert_eq!(broker_stats.get_stats_hour().get_avgpt(), 10.0);
    }

    #[test]
    fn test_set_stats_day() {
        let stats_minute = BrokerStatsItem::new(100, 12.5, 8.3);
        let stats_hour = BrokerStatsItem::new(500, 15.0, 9.0);
        let stats_day = BrokerStatsItem::new(1000, 20.0, 10.0);

        let mut broker_stats = BrokerStatsData::new(stats_minute, stats_hour, stats_day);

        let new_stats_day = BrokerStatsItem::new(1200, 22.0, 11.0);
        broker_stats.set_stats_day(new_stats_day.clone());

        assert_eq!(broker_stats.get_stats_day().get_sum(), 1200);
        assert_eq!(broker_stats.get_stats_day().get_tps(), 22.0);
        assert_eq!(broker_stats.get_stats_day().get_avgpt(), 11.0);
    }
}
