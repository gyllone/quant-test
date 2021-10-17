mod tick;
mod transaction;
mod raw_data;
mod strategy;
mod utils;

use std::time::SystemTime;
use raw_data::{parse_ticks_from_file, parse_transactions_from_file};
use strategy::{StrategyContext, StrategyConfig};

fn main() {
    let start = SystemTime::now();
    let config = StrategyConfig::new_from_file("./resource/strategy-config.toml").expect("load config error");
    let ticks = parse_ticks_from_file("./resource/601012.SH.Tick.csv").expect("parse ticks error");
    let transactions = parse_transactions_from_file("./resource/601012.SH.Transaction.csv").expect("parse transactions error");
    let elapsed = SystemTime::now().duration_since(start).unwrap();
    println!("load data used: {:?}\n", elapsed);

    let res = StrategyContext {
        ticks,
        transactions,
        config,
    }.process();
    println!("{}", res);
}
