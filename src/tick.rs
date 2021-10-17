use std::ops::Neg;

use anyhow::{anyhow, Error};
use crate::transaction::Transaction;
use crate::utils::{time_unparser, Price, Volume, Value, Time, Direction};

#[derive(Debug)]
pub struct Tick {
    pub timestamp: Time,
    pub new_price: Price,
    pub asks: Vec<(Price, Volume)>,
    pub bids: Vec<(Price, Volume)>,
    pub high_limited: Price,
    pub low_limited: Price,
}

const AM_START: Time = 34200000;
const AM_END: Time = 41400000;
const PM_START: Time = 46800000;
const PM_END: Time = 54000000;

impl Tick {
    pub fn time_eplased(&self, other: &Self) -> Time {
        self.timestamp - other.timestamp
    }

    pub fn in_trading_time(&self) -> bool {
        (self.timestamp >= AM_START && self.timestamp <= AM_END) ||
            (self.timestamp >= PM_START && self.timestamp <= PM_END)
    }

    #[allow(dead_code)]
    pub fn is_price_valid(&self, price: Price, direction: Direction) -> bool {
        match direction {
            Direction::Buy => price < self.high_limited,
            Direction::Sell => price > self.low_limited,
        }
    }

    pub fn get_first_ask_price(&self) -> Option<Price> {
        self.asks.first().map(|(price, _)| *price)
    }

    #[allow(dead_code)]
    pub fn get_first_bid_price(&self) -> Option<Price> {
        self.bids.first().map(|(price, _)| *price)
    }

    pub fn handle_limit_order_by_transaction(
        &self,
        price: Price,
        volume: Volume,
        direction: Direction,
        transaction: &Transaction,
    ) -> Volume {
        assert_ne!(volume, 0, "volume of limit order should not be zero");
        match direction {
            Direction::Buy => {
                match transaction.direction {
                    Direction::Buy => volume,
                    Direction::Sell => {
                        let mut orders = self.asks.clone();
                        let special_idx = orders.binary_search_by_key(&price, |(p, _)| *p);
                        match special_idx {
                            Ok(index) => {
                                orders[index].1 += volume;
                                let volume_before = orders[index].1;
                                transaction.handle(&mut orders);

                                (orders[index].1 * volume) / volume_before
                            }
                            Err(index) => {
                                orders.insert(index, (price, volume));
                                transaction.handle(&mut orders);

                                orders[index].1
                            }
                        }
                    }
                }
            }
            Direction::Sell => {
                match transaction.direction {
                    Direction::Buy => {
                        let prices = self.bids
                            .iter()
                            .map(|(p, _)| (*p as isize).neg())
                            .collect::<Vec<_>>();
                        let mut orders = self.bids.clone();
                        match prices.binary_search_by_key(&(price as isize).neg(), |p| *p) {
                            Ok(index) => {
                                orders[index].1 += volume;
                                let volume_before = orders[index].1;
                                transaction.handle(&mut orders);
                                
                                (orders[index].1 * volume) / volume_before
                            }
                            Err(index) => {
                                orders.insert(index, (price, volume));
                                transaction.handle(&mut orders);

                                orders[index].1
                            }
                        }
                    }
                    Direction::Sell => volume,
                }
            }
        }
    }

    pub fn handle_market_order(
        &self,
        volume: usize,
        direction: Direction,
    ) -> Result<(Price, Value), Error> {
        assert_ne!(volume, 0, "volume of market order should not be zero");
        if !self.in_trading_time() {
            return Err(anyhow!("market order at {} is not in trading time", time_unparser(self.timestamp)));
        }

        let orders_iter = match direction {
            Direction::Buy => self.asks.iter(),
            Direction::Sell => self.bids.iter(),
        };

        let mut value = 0;
        let mut left_volume = volume;
        for (p, v) in orders_iter {
            if v >= &left_volume {
                value += p * left_volume;
                break;
            } else {
                left_volume -= v;
                value += p * v;
            }
        }

        Ok((value / volume, value))
    }
}

