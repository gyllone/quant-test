pub type Price = usize;
pub type Volume = usize;
pub type Value = usize;
pub type Time = i64;

#[derive(Debug)]
pub enum Direction {
    Buy,
    Sell,
}

impl From<&str> for Direction {
    fn from(value: &str) -> Self {
        match value {
            "B" => Self::Buy,
            "S" => Self::Sell,
            _ => panic!("unexpected direction"),
        }
    }
}

pub fn time_parser(mut t: usize) -> Time {
    let m_secs = t % 1000;
    t /= 1000;
    let secs = t % 100;
    t /= 100;
    let mins = t % 100;
    t /= 100;
    let hours = t;

    ((hours * 3600 + mins * 60 + secs) * 1000 + m_secs) as Time
}

pub fn time_unparser(mut t: Time) -> usize {
    let m_secs = t % 1000;
    t /= 1000;
    let hours = t / 3600;
    t = t % 3600;
    let mins = t / 60;
    t = t % 60;
    let secs = t ;

    ((hours * 10000 + mins * 100 + secs) * 1000 + m_secs) as usize
}