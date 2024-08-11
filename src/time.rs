use std::time::{self, Duration, SystemTime};

use fuser::TimeOrNow;

#[derive(Clone, Copy, Debug)]
pub struct TimeSpec {
    pub secs: u64,
    pub nanos: u32,
}

impl TimeSpec {
    pub fn new(secs: u64, nanos: u32) -> Self {
        Self { secs, nanos }
    }
}

impl From<SystemTime> for TimeSpec {
    fn from(value: SystemTime) -> Self {
        let d = value
            .duration_since(time::UNIX_EPOCH)
            .expect("Time is before UNIX epoch");
        TimeSpec {
            secs: d.as_secs(),
            nanos: d.subsec_nanos(),
        }
    }
}

impl From<TimeSpec> for SystemTime {
    fn from(val: TimeSpec) -> Self {
        time::UNIX_EPOCH + Duration::new(val.secs, val.nanos)
    }
}

impl From<TimeOrNow> for TimeSpec {
    fn from(value: TimeOrNow) -> Self {
        match value {
            TimeOrNow::SpecificTime(t) => t.into(),
            TimeOrNow::Now => SystemTime::now().into(),
        }
    }
}

impl From<TimeSpec> for TimeOrNow {
    fn from(value: TimeSpec) -> Self {
        TimeOrNow::SpecificTime(value.into())
    }
}
