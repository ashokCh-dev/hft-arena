// HFT Arena — Rust reference matching engine (self-contained submission).
//
// Price-time-priority limit order book over the WS-JSON contract on :9000.
//   in : {"t":"limit","id":N,"side":"buy"|"sell","px":P,"qty":Q,"ts":...}
//        {"t":"market","id":N,"side":...,"qty":Q}
//        {"t":"cancel","id":N,"target":M}
//   out: {"ack":N,"ts":<ns>}                          (sent first; latency target)
//        {"fill":N,"px":P,"qty":Q,"maker":M}
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

// Each price level is a FIFO VecDeque (time priority); an index maps order ids to
// their (side, price) for cancels.
struct Book {
    bids: BTreeMap<i64, VecDeque<(u64, i64)>>,
    asks: BTreeMap<i64, VecDeque<(u64, i64)>>,
    index: HashMap<u64, (u8, i64)>, // id -> (0=bid, 1=ask, price)
}

impl Book {
    fn new() -> Self {
        Book { bids: BTreeMap::new(), asks: BTreeMap::new(), index: HashMap::new() }
    }

    fn rest(&mut self, id: u64, buy: bool, px: i64, qty: i64) {
        let (levels, side) = if buy { (&mut self.bids, 0u8) } else { (&mut self.asks, 1u8) };
        levels.entry(px).or_default().push_back((id, qty));
        self.index.insert(id, (side, px));
    }

    fn match_order(&mut self, buy: bool, limit_px: i64, mut qty: i64, has_limit: bool)
        -> (Vec<(i64, i64, u64)>, i64) {
        let mut fills = Vec::new();
        let mut filled_ids = Vec::new();
        loop {
            if qty <= 0 { break; }
            let best = if buy { self.asks.keys().next().copied() }
                       else { self.bids.keys().next_back().copied() };
            let best = match best { Some(b) => b, None => break };
            if has_limit && ((buy && best > limit_px) || (!buy && best < limit_px)) { break; }
            {
                let levels = if buy { &mut self.asks } else { &mut self.bids };
                let dq = levels.get_mut(&best).unwrap();
                while qty > 0 {
                    match dq.front_mut() {
                        Some(front) => {
                            let trade = qty.min(front.1);   // oldest first = time priority
                            fills.push((best, trade, front.0));
                            qty -= trade;
                            front.1 -= trade;
                            if front.1 == 0 {
                                filled_ids.push(dq.pop_front().unwrap().0);
                            }
                        }
                        None => break,
                    }
                }
                if dq.is_empty() { levels.remove(&best); }
            }
        }
        for id in filled_ids { self.index.remove(&id); }
        (fills, qty)
    }

    fn limit(&mut self, id: u64, buy: bool, px: i64, qty: i64) -> Vec<(i64, i64, u64)> {
        let (fills, rem) = self.match_order(buy, px, qty, true);
        if rem > 0 { self.rest(id, buy, px, rem); }
        fills
    }

    fn market(&mut self, buy: bool, qty: i64) -> Vec<(i64, i64, u64)> {
        self.match_order(buy, 0, qty, false).0
    }

    fn cancel(&mut self, target: u64) {
        if let Some((side, px)) = self.index.remove(&target) {
            let levels = if side == 0 { &mut self.bids } else { &mut self.asks };
            if let Some(dq) = levels.get_mut(&px) {
                dq.retain(|&(id, _)| id != target);
                if dq.is_empty() { levels.remove(&px); }
            }
        }
    }
}

fn now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
}

#[tokio::main]
async fn main() {
    let book = Arc::new(Mutex::new(Book::new()));
    let listener = TcpListener::bind("0.0.0.0:9000").await.unwrap();
    println!("[reference_engine_rust] price-time book listening on :9000");
    loop {
        let (stream, _) = match listener.accept().await { Ok(x) => x, Err(_) => continue };
        let book = book.clone();
        tokio::spawn(async move {
            let ws = match tokio_tungstenite::accept_async(stream).await {
                Ok(w) => w, Err(_) => return,
            };
            let (mut write, mut read) = ws.split();
            while let Some(Ok(msg)) = read.next().await {
                if let Message::Text(txt) = msg {
                    let v: Value = match serde_json::from_str(&txt) { Ok(x) => x, Err(_) => continue };
                    let id = v["id"].as_u64().unwrap_or(0);
                    // Ack first — the bot measures latency to this.
                    let _ = write.send(Message::Text(
                        format!("{{\"ack\":{},\"ts\":{}}}", id, now_ns()))).await;
                    let buy = v["side"].as_str() == Some("buy");
                    let fills = {
                        let mut b = book.lock().await;
                        match v["t"].as_str().unwrap_or("") {
                            "limit" => b.limit(id, buy, v["px"].as_i64().unwrap_or(0),
                                               v["qty"].as_i64().unwrap_or(0)),
                            "market" => b.market(buy, v["qty"].as_i64().unwrap_or(0)),
                            "cancel" => { b.cancel(v["target"].as_u64().unwrap_or(0)); Vec::new() }
                            _ => Vec::new(),
                        }
                    };
                    for (px, q, maker) in fills {
                        let _ = write.send(Message::Text(format!(
                            "{{\"fill\":{},\"px\":{},\"qty\":{},\"maker\":{}}}", id, px, q, maker))).await;
                    }
                }
            }
        });
    }
}
