use crate::utils::{Direction, Price, Volume};

#[derive(Debug)]
pub struct Transaction {
    pub timestamp: i64,
    pub index: usize,
    pub price: usize,
    pub volume: usize,
    pub direction: Direction,
}

impl Transaction {
    pub fn handle(
        &self,
        orders: &mut [(Price, Volume)],
    ) {
        let mut trx_volume = self.volume;
        for (price, volume) in orders.iter_mut() {
            if match self.direction {
                Direction::Buy => &self.price < price,
                Direction::Sell => &self.price > price,
            } {
                break;
            }
            if &trx_volume > volume {
                trx_volume -= *volume;
                *volume = 0;
            } else {
                *volume -= trx_volume;
                break;
            }
        }
    }
}
