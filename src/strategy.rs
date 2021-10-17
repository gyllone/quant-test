use std::sync::{Arc, Mutex};
use std::fmt;
use std::time::{Duration, SystemTime};
use config::{Config, ConfigError, File};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::tick::Tick;
use crate::transaction::Transaction;
use crate::utils::{Direction, Price, Time, Value, Volume};

#[derive(Debug)]
pub struct Order {
    timestamp: Time,
    price: Price,
    volume: Volume,
    value: Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(default)]
struct StrategyRawConfig {
    pub rise_duration_min: i32,
    pub rise_threshold_percent: f64,
    pub open_volume: usize,
    pub open_min_interval_sec: i32,
    pub limit_close_elapsed_sec: i32,
    pub close_waiting_elapsed_sec: i32,
    pub active_fee_ratio: f64,
    pub passive_fee_ratio: f64,
}

impl Default for StrategyRawConfig {
    fn default() -> Self {
        Self {
            rise_duration_min: 10,
            rise_threshold_percent: 0.5,
            open_volume: 1000,
            open_min_interval_sec: 30,
            limit_close_elapsed_sec: 60,
            close_waiting_elapsed_sec: 30,
            active_fee_ratio: 0.02f64,
            passive_fee_ratio: 0.015f64,
        }
    }
}

#[derive(Debug)]
pub struct StrategyConfig {
    rise_duration: Time,
    rise_threshold: f64,
    open_volume: Volume,
    open_min_interval: Time,
    limit_close_elapsed: Time,
    close_waiting_elapsed: Time,
    active_fee_ratio: f64,
    passive_fee_ratio: f64,
}

impl From<StrategyRawConfig> for StrategyConfig {
    fn from(config: StrategyRawConfig) -> Self {
        Self {
            rise_duration: config.rise_duration_min as Time * 60 * 1000,
            rise_threshold: config.rise_threshold_percent / 100f64,
            open_volume: config.open_volume,
            open_min_interval: config.open_min_interval_sec as Time * 1000,
            limit_close_elapsed: config.limit_close_elapsed_sec as Time * 1000,
            close_waiting_elapsed: config.close_waiting_elapsed_sec as Time * 1000,
            active_fee_ratio: config.active_fee_ratio / 100f64,
            passive_fee_ratio: config.passive_fee_ratio / 100f64,
        }
    }
}

impl StrategyConfig {
    pub fn new_from_file(path: &str) -> Result<StrategyConfig, ConfigError> {
        let mut s = Config::new();
        s.merge(File::with_name(path).required(false))?;

        Ok(StrategyConfig::from(s.try_into::<StrategyRawConfig>()?))
    }
}

#[derive(Debug)]
pub struct StrategyContext {
    pub ticks: Vec<Tick>,
    pub transactions: Vec<Transaction>,
    pub config: StrategyConfig,
}

pub struct StrategyResult {
    pub open_times: usize,
    pub open_value: Value,
    pub close_active_traded_times: usize,
    pub close_active_traded_value: Value,
    pub close_passive_traded_times: usize,
    pub close_passive_traded_value: Value,
    pub yield_rate: f64,
    pub time_elapsed: Duration,
}

impl StrategyResult {
    pub fn new(
        open_orders: &[(usize, Order)],
        active_traded_orders: &[Order],
        passive_traded_orders: &[Order],
        time_elapsed: Duration,
    ) -> StrategyResult {
        let open_times = open_orders.len();
        let open_value = open_orders
            .iter()
            .fold(0usize, |acc, (_, order)| acc + order.value);

        let close_active_traded_times = active_traded_orders.len();
        let close_active_traded_value = active_traded_orders
            .iter()
            .fold(0usize, |acc, order| acc + order.value);

        let close_passive_traded_times = passive_traded_orders.len();
        let close_passive_traded_value = passive_traded_orders
            .iter()
            .fold(0usize, |acc, order| acc + order.value);

        let yield_rate = ((close_active_traded_value + close_passive_traded_value) as f64 - open_value as f64)/ open_value as f64;

        StrategyResult {
            open_times,
            open_value,
            close_active_traded_times,
            close_active_traded_value,
            close_passive_traded_times,
            close_passive_traded_value,
            yield_rate,
            time_elapsed,
        }
    }
}

impl fmt::Display for StrategyResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[Test Result]\ntime used: {:?}\nyield rate: {:.4}%\nopen:
            times: {}
            value: {}\nclose:
            active:
            \ttimes: {}
            \tvalue: {}
            passive:
            \ttimes: {}
            \tvalue: {}
            ",
            self.time_elapsed,
            self.yield_rate * 100f64,
            self.open_times,
            self.open_value,
            self.close_active_traded_times,
            self.close_active_traded_value,
            self.close_passive_traded_times,
            self.close_passive_traded_value,
        )
    }
}

impl StrategyContext {
    fn open_trigger(&self, index: usize, open_tick: &Tick, last_open: Time) -> bool {
        if !open_tick.in_trading_time() || open_tick.timestamp - last_open <= self.config.open_min_interval {
            return false;
        }
        let expect_price = (open_tick.new_price as f64) * (1f64 + self.config.rise_threshold);
        for tick in self.ticks[..index].iter().rev() {
            if open_tick.time_eplased(tick) > self.config.rise_duration {
                break;
            }
            if tick.new_price as f64 >= expect_price {
                return true;
            }
        }
        return false;
    }

    fn open_market_orders(&self) -> Vec<(usize, Order)> {
        let mut market_open_orders = Vec::new();
        let mut last_open: Time = 0;
        // opening orders
        self.ticks
            .iter()
            .enumerate()
            .for_each(|(index, tick)| {
                if self.open_trigger(index, tick, last_open) {
                    let volume = self.config.open_volume;
                    match tick.handle_market_order(volume, Direction::Buy) {
                        Ok((price, value)) => {
                            market_open_orders.push((
                                index,
                                Order {
                                    timestamp: tick.timestamp,
                                    price,
                                    volume,
                                    value,
                                },
                            ));
                            last_open = tick.timestamp;
                        }
                        Err(e) => println!("error: {:?}, volume: {}", e, volume),
                    }
                }
            });

        market_open_orders
    }

    fn close_limit_pending_orders(&self, market_open_orders: &[(usize, Order)]) -> Arc<Mutex<Vec<(usize, Order)>>> {
        let limit_pending_orders = Arc::new(Mutex::new(Vec::new()));
        market_open_orders
            .par_iter()
            .for_each(|(index, opened_order)| {
                let limit_pending_orders = limit_pending_orders.clone();
                for (idx, tick) in self.ticks[*index..].iter().enumerate() {
                    if tick.timestamp > opened_order.timestamp + self.config.limit_close_elapsed {
                        let price = tick.get_first_ask_price().expect("ask plate is empty");
                        let mut limit_pending_orders = limit_pending_orders.lock().unwrap();
                        limit_pending_orders.push((
                            *index + idx,
                            Order {
                                timestamp: tick.timestamp,
                                price,
                                volume: opened_order.volume,
                                value: price * opened_order.volume,
                            },
                        ));
                        break;
                    }
                }
            });

        limit_pending_orders
            .lock()
            .unwrap()
            .sort_by_key(|(_, order)| order.timestamp);

        limit_pending_orders
    }

    fn close_all_orders(&self, pending_orders: Arc<Mutex<Vec<(usize, Order)>>>) -> (Arc<Mutex<Vec<Order>>>, Arc<Mutex<Vec<Order>>>) {
        let active_traded_orders = Arc::new(Mutex::new(Vec::new()));
        let passive_traded_orders = Arc::new(Mutex::new(Vec::new()));
        pending_orders
            .lock()
            .unwrap()
            .par_iter()
            .for_each(|(index, pending_order)| {
                let active_traded_orders = active_traded_orders.clone();
                let passive_traded_orders = passive_traded_orders.clone();
                let mut current_volume = pending_order.volume;
                let mut ticks_iter = self.ticks[*index..].iter().peekable();
                loop {
                    if current_volume == 0 {
                        break;
                    }
                    if let Some(tick) = ticks_iter.next() {
                        // sell by market
                        if tick.timestamp > pending_order.timestamp + self.config.close_waiting_elapsed {
                            match tick.handle_market_order(current_volume, Direction::Sell) {
                                Ok((price, value)) => {
                                    let mut active_traded_orders = active_traded_orders.lock().unwrap();
                                    active_traded_orders.push(Order {
                                        timestamp: tick.timestamp,
                                        price,
                                        volume: current_volume,
                                        value: (value as f64 * (1f64 - self.config.active_fee_ratio)) as Value,
                                    });
                                    break;
                                }
                                Err(e) => println!("{:?}", e),
                            }
                        }
                        if let Some(next_tick) = ticks_iter.peek() {
                            let tx_index =
                                match self.transactions.binary_search_by_key(&tick.timestamp, |tx| tx.timestamp) {
                                    Ok(i) => i + 1,
                                    Err(i) => i,
                                };
                            for transaction in
                                self.transactions
                                    .get(tx_index..)
                                    .unwrap_or_default()
                                    .iter()
                            {
                                if current_volume == 0 || transaction.timestamp >= next_tick.timestamp {
                                    break;
                                }
                                let rest_volume = tick.handle_limit_order_by_transaction(
                                    pending_order.price,
                                    current_volume,
                                    Direction::Sell,
                                    transaction,
                                );
                                if rest_volume < current_volume {
                                    let mut passive_traded_orders = passive_traded_orders.lock().unwrap();
                                    let volume = current_volume - rest_volume;
                                    let value = ((pending_order.price * volume) as f64 * (1f64 - self.config.passive_fee_ratio)) as Value;
                                    passive_traded_orders.push(Order {
                                        timestamp: transaction.timestamp,
                                        price: pending_order.price,
                                        volume,
                                        value,
                                    });
                                    current_volume = rest_volume;
                                }
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
            });

        active_traded_orders
            .lock()
            .unwrap()
            .sort_by_key(|order| order.timestamp);
            
        passive_traded_orders
            .lock()
            .unwrap()
            .sort_by_key(|order| order.timestamp);

        (active_traded_orders, passive_traded_orders)
    }

    pub fn process(&self) -> StrategyResult {
        let start = SystemTime::now();

        let market_open_orders = self.open_market_orders();
        let pending_orders = self.close_limit_pending_orders(&market_open_orders);
        let (active_traded_orders, passive_traded_orders) = self.close_all_orders(pending_orders);

        let active_traded_orders = active_traded_orders.lock().unwrap();
        let passive_traded_orders = passive_traded_orders.lock().unwrap();
        let elapsed = SystemTime::now().duration_since(start).unwrap();
        StrategyResult::new(
            &market_open_orders,
            &active_traded_orders,
            &passive_traded_orders,
            elapsed,
        )
    }
}