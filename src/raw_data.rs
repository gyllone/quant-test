use anyhow::Error;
use std::path::Path;
use serde::Deserialize;
use crate::tick::Tick;
use crate::transaction::Transaction;
use crate::utils::{time_parser, Direction};

#[derive(Debug, Deserialize)]
pub struct TickRawData {
    #[serde(rename = "chWindCode")]
    pub ch_wind_code: String,
    #[serde(rename = "nTime")]
    pub n_time: usize,
    #[serde(rename = "Status")]
    pub status: usize,
    #[serde(rename = "PreClose")]
    pub pre_close: usize,
    #[serde(rename = "Open")]
    pub open: usize,
    #[serde(rename = "High")]
    pub high: usize,
    #[serde(rename = "Low")]
    pub low: usize,
    #[serde(rename = "nPrice")]
    pub n_price: usize,
    #[serde(rename = "nAskPrice1")]
    pub n_ask_price_1: usize,
    #[serde(rename = "nAskPrice2")]
    pub n_ask_price_2: usize,
    #[serde(rename = "nAskPrice3")]
    pub n_ask_price_3: usize,
    #[serde(rename = "nAskPrice4")]
    pub n_ask_price_4: usize,
    #[serde(rename = "nAskPrice5")]
    pub n_ask_price_5: usize,
    #[serde(rename = "nAskPrice6")]
    pub n_ask_price_6: usize,
    #[serde(rename = "nAskPrice7")]
    pub n_ask_price_7: usize,
    #[serde(rename = "nAskPrice8")]
    pub n_ask_price_8: usize,
    #[serde(rename = "nAskPrice9")]
    pub n_ask_price_9: usize,
    #[serde(rename = "nAskPrice10")]
    pub n_ask_price_10: usize,
    #[serde(rename = "nAskVolume1")]
    pub n_ask_volume_1: usize,
    #[serde(rename = "nAskVolume2")]
    pub n_ask_volume_2: usize,
    #[serde(rename = "nAskVolume3")]
    pub n_ask_volume_3: usize,
    #[serde(rename = "nAskVolume4")]
    pub n_ask_volume_4: usize,
    #[serde(rename = "nAskVolume5")]
    pub n_ask_volume_5: usize,
    #[serde(rename = "nAskVolume6")]
    pub n_ask_volume_6: usize,
    #[serde(rename = "nAskVolume7")]
    pub n_ask_volume_7: usize,
    #[serde(rename = "nAskVolume8")]
    pub n_ask_volume_8: usize,
    #[serde(rename = "nAskVolume9")]
    pub n_ask_volume_9: usize,
    #[serde(rename = "nAskVolume10")]
    pub n_ask_volume_10: usize,
    #[serde(rename = "nBidPrice1")]
    pub n_bid_price_1: usize,
    #[serde(rename = "nBidPrice2")]
    pub n_bid_price_2: usize,
    #[serde(rename = "nBidPrice3")]
    pub n_bid_price_3: usize,
    #[serde(rename = "nBidPrice4")]
    pub n_bid_price_4: usize,
    #[serde(rename = "nBidPrice5")]
    pub n_bid_price_5: usize,
    #[serde(rename = "nBidPrice6")]
    pub n_bid_price_6: usize,
    #[serde(rename = "nBidPrice7")]
    pub n_bid_price_7: usize,
    #[serde(rename = "nBidPrice8")]
    pub n_bid_price_8: usize,
    #[serde(rename = "nBidPrice9")]
    pub n_bid_price_9: usize,
    #[serde(rename = "nBidPrice10")]
    pub n_bid_price_10: usize,
    #[serde(rename = "nBidVolume1")]
    pub n_bid_volume_1: usize,
    #[serde(rename = "nBidVolume2")]
    pub n_bid_volume_2: usize,
    #[serde(rename = "nBidVolume3")]
    pub n_bid_volume_3: usize,
    #[serde(rename = "nBidVolume4")]
    pub n_bid_volume_4: usize,
    #[serde(rename = "nBidVolume5")]
    pub n_bid_volume_5: usize,
    #[serde(rename = "nBidVolume6")]
    pub n_bid_volume_6: usize,
    #[serde(rename = "nBidVolume7")]
    pub n_bid_volume_7: usize,
    #[serde(rename = "nBidVolume8")]
    pub n_bid_volume_8: usize,
    #[serde(rename = "nBidVolume9")]
    pub n_bid_volume_9: usize,
    #[serde(rename = "nBidVolume10")]
    pub n_bid_volume_10: usize,
    #[serde(rename = "nMatchItems")]
    pub n_match_items: usize,
    #[serde(rename = "TotalVolume")]
    pub total_volume: usize,
    #[serde(rename = "TotalTurnover")]
    pub total_turnover: usize,
    #[serde(rename = "TotalBidVolume")]
    pub total_bid_volume: usize,
    #[serde(rename = "TotalAskVolume")]
    pub total_ask_volume: usize,
    #[serde(rename = "WeightedAvgBidPrice")]
    pub weighted_avg_bid_price: usize,
    #[serde(rename = "WeightedAvgAskPrice")]
    pub weighted_avg_ask_price: usize,
    #[serde(rename = "IOPV")]
    pub iopv: usize,
    #[serde(rename = "YieldToMaturity")]
    pub yield_to_maturity: usize,
    #[serde(rename = "HighLimited")]
    pub high_limited: usize,
    #[serde(rename = "LowLimited")]
    pub low_limited: usize,
}

#[derive(Debug, Deserialize)]
pub struct TrxRawData {
    #[serde(rename = "Tkr")]
    pub tkr: String,
    #[serde(rename = "Time")]
    pub time: usize,
    #[serde(rename = "Index")]
    pub index: usize,
    #[serde(rename = "Price")]
    pub price: usize,
    #[serde(rename = "Volume")]
    pub volume: usize,
    #[serde(rename = "Turnover")]
    pub turnover: usize,
    #[serde(rename = "BSFlag")]
    pub flag: String,
    #[serde(rename = "OrderKind")]
    pub order_kind: usize,
    #[serde(rename = "FunctionCode")]
    pub function_code: usize,
    #[serde(rename = "AskOrder")]
    pub ask_order: usize,
    #[serde(rename = "BidOrder")]
    pub bid_order: usize,
}

impl Into<Tick> for TickRawData {
    fn into(self) -> Tick {
        Tick {
            timestamp: time_parser(self.n_time),
            new_price: self.n_price,
            asks: vec![
                (self.n_ask_price_1, self.n_ask_volume_1),
                (self.n_ask_price_2, self.n_ask_volume_2),
                (self.n_ask_price_3, self.n_ask_volume_3),
                (self.n_ask_price_4, self.n_ask_volume_4),
                (self.n_ask_price_5, self.n_ask_volume_5),
                (self.n_ask_price_6, self.n_ask_volume_6),
                (self.n_ask_price_7, self.n_ask_volume_7),
                (self.n_ask_price_8, self.n_ask_volume_8),
                (self.n_ask_price_9, self.n_ask_volume_9),
                (self.n_ask_price_10, self.n_ask_volume_10),
            ],
            bids: vec![
                (self.n_bid_price_1, self.n_bid_volume_1),
                (self.n_bid_price_2, self.n_bid_volume_2),
                (self.n_bid_price_3, self.n_bid_volume_3),
                (self.n_bid_price_4, self.n_bid_volume_4),
                (self.n_bid_price_5, self.n_bid_volume_5),
                (self.n_bid_price_6, self.n_bid_volume_6),
                (self.n_bid_price_7, self.n_bid_volume_7),
                (self.n_bid_price_8, self.n_bid_volume_8),
                (self.n_bid_price_9, self.n_bid_volume_9),
                (self.n_bid_price_10, self.n_bid_volume_10),
            ],
            high_limited: self.high_limited,
            low_limited: self.low_limited,
        }
    }
}

impl Into<Transaction> for TrxRawData {
    fn into(self) -> Transaction {
        Transaction {
            timestamp: time_parser(self.time),
            index: self.index,
            price: self.price,
            volume: self.volume,
            direction: Direction::from(self.flag.as_str()),
        }
    }
}

pub fn parse_ticks_from_file(path: &str) -> Result<Vec<Tick>, Error> {
    let mut reader = csv::Reader::from_path(Path::new(path))?;
    let ticks = reader
        .deserialize::<TickRawData>()
        .into_iter()
        .map(|raw_data| Ok(raw_data?.into()))
        .collect::<Result<Vec<_>, csv::Error>>()?;

    Ok(ticks)
}

pub fn parse_transactions_from_file(path: &str) -> Result<Vec<Transaction>, Error> {
    let mut reader = csv::Reader::from_path(Path::new(path))?;
    let transactions = reader
        .deserialize::<TrxRawData>()
        .into_iter()
        .map(|raw_data| Ok(raw_data?.into()))
        .collect::<Result<Vec<_>, csv::Error>>()?;

    Ok(transactions)
}