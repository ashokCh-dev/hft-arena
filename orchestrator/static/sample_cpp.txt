// HFT Arena — C++ reference matching engine (self-contained submission).
//
// Seeded from hft_arena's contestant.cpp, but upgraded from a TCP binary-echo
// into a real price-time-priority order book that speaks the WS-JSON contract
// over crow.h's WebSocket (crow.h is provided by the cpp sandbox template).
//
//   bot  -> {"t":"limit","id":N,"side":"buy"|"sell","px":P,"qty":Q,"ts":...}
//           {"t":"market","id":N,"side":...,"qty":Q}
//           {"t":"cancel","id":N,"target":M}
//   engine-> {"ack":N,"ts":<ns>}                         (sent first; latency target)
//            {"fill":N,"px":P,"qty":Q,"maker":M}
//
#include "crow.h"
#include <chrono>
#include <cstdint>
#include <list>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>

static inline long long now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

struct Resting {
    uint64_t id;
    long qty;
};
struct Fill {
    long px;
    long qty;
    uint64_t maker;
};

// Price-time-priority limit order book.
// Each price level is a std::list (FIFO == time priority); an index maps every
// resting order id to its level + list iterator for O(1) cancels.
class OrderBook {
public:
    // bids keyed ascending (best = highest price = rbegin); asks ascending (best = begin)
    std::map<long, std::list<Resting>> bids, asks;
    struct Loc { char side; long price; std::list<Resting>::iterator it; };
    std::unordered_map<uint64_t, Loc> index;

    std::vector<Fill> limit(uint64_t id, bool buy, long px, long qty) {
        auto fills = match(buy, px, qty, /*has_limit=*/true);
        if (qty > 0) rest(id, buy, px, qty);
        return fills.first;
    }
    std::vector<Fill> market(uint64_t id, bool buy, long qty) {
        (void)id;
        return match(buy, 0, qty, /*has_limit=*/false).first;
    }
    bool cancel(uint64_t target) {
        auto f = index.find(target);
        if (f == index.end()) return false;
        auto &book = (f->second.side == 'b') ? bids : asks;
        auto lvl = book.find(f->second.price);
        if (lvl != book.end()) {
            lvl->second.erase(f->second.it);
            if (lvl->second.empty()) book.erase(lvl);
        }
        index.erase(f);
        return true;
    }

private:
    void rest(uint64_t id, bool buy, long px, long qty) {
        auto &book = buy ? bids : asks;
        auto &lvl = book[px];
        lvl.push_back({id, qty});
        index[id] = {buy ? 'b' : 'a', px, std::prev(lvl.end())};
    }

    // Returns (fills, remaining-qty-via-byref). `qty` is updated in place.
    std::pair<std::vector<Fill>, int> match(bool buy, long limit_px, long &qty, bool has_limit) {
        std::vector<Fill> fills;
        auto &opp = buy ? asks : bids;
        while (qty > 0 && !opp.empty()) {
            auto best_it = buy ? opp.begin() : std::prev(opp.end());
            long best = best_it->first;
            if (has_limit && (buy ? best > limit_px : best < limit_px)) break;
            auto &lvl = best_it->second;
            while (qty > 0 && !lvl.empty()) {
                Resting &m = lvl.front();
                long trade = qty < m.qty ? qty : m.qty;
                fills.push_back({best, trade, m.id});
                qty -= trade;
                m.qty -= trade;
                if (m.qty == 0) {
                    index.erase(m.id);
                    lvl.pop_front();
                }
            }
            if (lvl.empty()) opp.erase(best_it);
        }
        return {fills, 0};
    }
};

static OrderBook BOOK;
static std::mutex BOOK_MTX;

int main() {
    crow::SimpleApp app;
    crow::logger::setLogLevel(crow::LogLevel::Warning);

    CROW_ROUTE(app, "/")
        .websocket(&app)
        .onopen([](crow::websocket::connection &) {})
        .onclose([](crow::websocket::connection &, const std::string &, uint16_t) {})
        .onmessage([](crow::websocket::connection &conn, const std::string &data, bool) {
            auto m = crow::json::load(data);
            if (!m) return;
            uint64_t id = m["id"].u();

            // Ack first — the bot measures latency to this.
            conn.send_text("{\"ack\":" + std::to_string(id) +
                           ",\"ts\":" + std::to_string(now_ns()) + "}");

            std::string type = m["t"].s();
            std::vector<Fill> fills;
            {
                std::lock_guard<std::mutex> lk(BOOK_MTX);
                if (type == "limit") {
                    bool buy = std::string(m["side"].s()) == "buy";
                    fills = BOOK.limit(id, buy, m["px"].i(), m["qty"].i());
                } else if (type == "market") {
                    bool buy = std::string(m["side"].s()) == "buy";
                    fills = BOOK.market(id, buy, m["qty"].i());
                } else if (type == "cancel") {
                    BOOK.cancel(m["target"].u());
                }
            }
            for (auto &f : fills) {
                conn.send_text("{\"fill\":" + std::to_string(id) +
                               ",\"px\":" + std::to_string(f.px) +
                               ",\"qty\":" + std::to_string(f.qty) +
                               ",\"maker\":" + std::to_string(f.maker) + "}");
            }
        });

    std::printf("[reference_engine_cpp] price-time book listening on :9000\n");
    std::fflush(stdout);
    app.port(9000).multithreaded().run();
    return 0;
}
