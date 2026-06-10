// HFT Arena — Go reference matching engine (self-contained submission).
//
// Price-time-priority limit order book over the WS-JSON contract on :9000.
//   in : {"t":"limit","id":N,"side":"buy"|"sell","px":P,"qty":Q,"ts":...}
//        {"t":"market","id":N,"side":...,"qty":Q}
//        {"t":"cancel","id":N,"target":M}
//   out: {"ack":N,"ts":<ns>}                          (sent first; latency target)
//        {"fill":N,"px":P,"qty":Q,"maker":M}
package main

import (
	"container/list"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type order struct {
	id  uint64
	qty int
}

type loc struct {
	side  byte // 'b' or 'a'
	price int
	el    *list.Element
}

// Book: each price level is a FIFO list (time priority); an index maps order ids
// to their level + element for O(1) cancels.
type Book struct {
	mu        sync.Mutex
	bidLevels map[int]*list.List
	askLevels map[int]*list.List
	bidPrices []int // ascending; best bid = last
	askPrices []int // ascending; best ask = first
	index     map[uint64]loc
}

func newBook() *Book {
	return &Book{
		bidLevels: map[int]*list.List{}, askLevels: map[int]*list.List{},
		index: map[uint64]loc{},
	}
}

type fill struct {
	px, qty int
	maker   uint64
}

func insort(s []int, v int) []int {
	i := sort.SearchInts(s, v)
	s = append(s, 0)
	copy(s[i+1:], s[i:])
	s[i] = v
	return s
}
func remove(s []int, v int) []int {
	i := sort.SearchInts(s, v)
	if i < len(s) && s[i] == v {
		return append(s[:i], s[i+1:]...)
	}
	return s
}

func (b *Book) rest(id uint64, buy bool, px, qty int) {
	levels := b.askLevels
	side := byte('a')
	if buy {
		levels, side = b.bidLevels, 'b'
	}
	l := levels[px]
	if l == nil {
		l = list.New()
		levels[px] = l
		if buy {
			b.bidPrices = insort(b.bidPrices, px)
		} else {
			b.askPrices = insort(b.askPrices, px)
		}
	}
	el := l.PushBack(&order{id, qty})
	b.index[id] = loc{side, px, el}
}

func (b *Book) match(buy bool, limitPx, qty int, hasLimit bool) ([]fill, int) {
	var fills []fill
	opp, prices := b.askLevels, &b.askPrices
	if !buy {
		opp, prices = b.bidLevels, &b.bidPrices
	}
	for qty > 0 && len(*prices) > 0 {
		var best int
		if buy {
			best = (*prices)[0] // lowest ask
			if hasLimit && best > limitPx {
				break
			}
		} else {
			best = (*prices)[len(*prices)-1] // highest bid
			if hasLimit && best < limitPx {
				break
			}
		}
		l := opp[best]
		for qty > 0 && l.Len() > 0 {
			e := l.Front() // oldest == time priority
			m := e.Value.(*order)
			trade := qty
			if m.qty < trade {
				trade = m.qty
			}
			fills = append(fills, fill{best, trade, m.id})
			qty -= trade
			m.qty -= trade
			if m.qty == 0 {
				l.Remove(e)
				delete(b.index, m.id)
			}
		}
		if l.Len() == 0 {
			delete(opp, best)
			*prices = remove(*prices, best)
		}
	}
	return fills, qty
}

func (b *Book) limit(id uint64, buy bool, px, qty int) []fill {
	fills, rem := b.match(buy, px, qty, true)
	if rem > 0 {
		b.rest(id, buy, px, rem)
	}
	return fills
}

func (b *Book) market(id uint64, buy bool, qty int) []fill {
	fills, _ := b.match(buy, 0, qty, false)
	return fills
}

func (b *Book) cancel(target uint64) {
	lc, ok := b.index[target]
	if !ok {
		return
	}
	levels := b.askLevels
	if lc.side == 'b' {
		levels = b.bidLevels
	}
	if l := levels[lc.price]; l != nil {
		l.Remove(lc.el)
		if l.Len() == 0 {
			delete(levels, lc.price)
			if lc.side == 'b' {
				b.bidPrices = remove(b.bidPrices, lc.price)
			} else {
				b.askPrices = remove(b.askPrices, lc.price)
			}
		}
	}
	delete(b.index, target)
}

type msg struct {
	T      string `json:"t"`
	ID     uint64 `json:"id"`
	Side   string `json:"side"`
	Px     int    `json:"px"`
	Qty    int    `json:"qty"`
	Target uint64 `json:"target"`
}

var book = newBook()
var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func handle(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	for {
		_, data, err := c.ReadMessage()
		if err != nil {
			return
		}
		var m msg
		if json.Unmarshal(data, &m) != nil {
			continue
		}
		// Ack first — the bot measures latency to this.
		c.WriteMessage(websocket.TextMessage,
			[]byte(`{"ack":`+strconv.FormatUint(m.ID, 10)+`,"ts":`+
				strconv.FormatInt(time.Now().UnixNano(), 10)+`}`))

		var fills []fill
		buy := m.Side == "buy"
		book.mu.Lock()
		switch m.T {
		case "limit":
			fills = book.limit(m.ID, buy, m.Px, m.Qty)
		case "market":
			fills = book.market(m.ID, buy, m.Qty)
		case "cancel":
			book.cancel(m.Target)
		}
		book.mu.Unlock()
		for _, f := range fills {
			c.WriteMessage(websocket.TextMessage, []byte(
				`{"fill":`+strconv.FormatUint(m.ID, 10)+`,"px":`+strconv.Itoa(f.px)+
					`,"qty":`+strconv.Itoa(f.qty)+`,"maker":`+strconv.FormatUint(f.maker, 10)+`}`))
		}
	}
}

func main() {
	http.HandleFunc("/", handle)
	println("[reference_engine_go] price-time book listening on :9000")
	http.ListenAndServe(":9000", nil)
}
