package orderbook

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/tent-of-trials/market/types"
)

type Config struct {
	MaxDepth       int
	PriceDecimals  int32
	VolumeDecimals int32
}

type OrderBook struct {
	mu        sync.RWMutex
	symbol    types.Symbol
	config    Config
	bids      []*types.Level
	asks      []*types.Level
	orders    map[string]*types.Order
	sequence  uint64
	updatedAt time.Time
	closed    bool
}

const snapshotVersion = 1

type orderBookSnapshot struct {
	Version  int            `json:"version"`
	Checksum string         `json:"checksum"`
	Body     orderBookState `json:"body"`
}

type orderBookState struct {
	Symbol    types.Symbol  `json:"symbol"`
	Config    Config        `json:"config"`
	Bids      []types.Level `json:"bids"`
	Asks      []types.Level `json:"asks"`
	Orders    []types.Order `json:"orders"`
	Sequence  uint64        `json:"sequence"`
	UpdatedAt time.Time     `json:"updated_at"`
}

func NewOrderBook(symbol types.Symbol, config Config) *OrderBook {
	return &OrderBook{
		symbol:   symbol,
		config:   config,
		bids:     make([]*types.Level, 0, config.MaxDepth),
		asks:     make([]*types.Level, 0, config.MaxDepth),
		orders:   make(map[string]*types.Order),
		sequence: 0,
	}
}

func (ob *OrderBook) Snapshot() ([]byte, error) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if ob.closed {
		return nil, ErrBookClosed
	}

	body := ob.snapshotStateLocked()
	return encodeSnapshot(body)
}

func (ob *OrderBook) Recover(data []byte) error {
	body, err := decodeSnapshot(data)
	if err != nil {
		return err
	}

	ob.mu.Lock()
	defer ob.mu.Unlock()

	if ob.closed {
		return ErrBookClosed
	}

	ob.symbol = body.Symbol
	if body.Config.MaxDepth > 0 {
		ob.config = body.Config
	}
	ob.bids = cloneLevels(body.Bids)
	ob.asks = cloneLevels(body.Asks)
	ob.orders = make(map[string]*types.Order, len(body.Orders))
	for _, order := range body.Orders {
		orderCopy := order
		if order.ExpireAt != nil {
			expireAt := *order.ExpireAt
			orderCopy.ExpireAt = &expireAt
		}
		ob.orders[orderCopy.ID] = &orderCopy
	}
	ob.sequence = body.Sequence
	ob.updatedAt = body.UpdatedAt

	sortLevels(ob.bids, true)
	sortLevels(ob.asks, false)

	return nil
}

func SnapshotSymbol(data []byte) (types.Symbol, error) {
	body, err := decodeSnapshot(data)
	if err != nil {
		return "", err
	}
	return body.Symbol, nil
}

func SnapshotChecksum(data []byte) (string, error) {
	var snapshot orderBookSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return "", err
	}
	if _, err := decodeSnapshot(data); err != nil {
		return "", err
	}
	return snapshot.Checksum, nil
}

func (ob *OrderBook) AddOrder(order *types.Order) ([]*types.Trade, error) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if ob.closed {
		return nil, ErrBookClosed
	}

	if order.ID == "" {
		order.ID = uuid.New().String()
	}

	order.CreatedAt = time.Now()
	order.UpdatedAt = time.Now()
	order.Status = types.New

	ob.orders[order.ID] = order
	ob.sequence++

	level := &types.Level{
		Price:    order.Price,
		Quantity: order.RemainingQty,
		Count:    1,
	}

	if order.Side == types.Buy {
		ob.bids = insertLevel(ob.bids, level, true)
	} else {
		ob.asks = insertLevel(ob.asks, level, false)
	}

	ob.updatedAt = time.Now()
	return nil, nil
}

func (ob *OrderBook) CancelOrder(orderID string) error {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if ob.closed {
		return ErrBookClosed
	}

	order, exists := ob.orders[orderID]
	if !exists {
		return ErrOrderNotFound
	}

	order.Status = types.Cancelled
	order.UpdatedAt = time.Now()
	delete(ob.orders, orderID)

	if order.Side == types.Buy {
		ob.bids = removeLevel(ob.bids, order.Price)
	} else {
		ob.asks = removeLevel(ob.asks, order.Price)
	}

	ob.updatedAt = time.Now()
	return nil
}

func (ob *OrderBook) GetBids() []*types.Level {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	result := make([]*types.Level, len(ob.bids))
	copy(result, ob.bids)
	return result
}

func (ob *OrderBook) GetAsks() []*types.Level {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	result := make([]*types.Level, len(ob.asks))
	copy(result, ob.asks)
	return result
}

func (ob *OrderBook) GetSnapshot() *types.DepthUpdate {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	bids := make([]types.Level, len(ob.bids))
	for i, l := range ob.bids {
		if l != nil {
			bids[i] = *l
		}
	}

	asks := make([]types.Level, len(ob.asks))
	for i, l := range ob.asks {
		if l != nil {
			asks[i] = *l
		}
	}

	return &types.DepthUpdate{
		Symbol:    ob.symbol,
		Bids:      bids,
		Asks:      asks,
		Timestamp: time.Now().UnixMilli(),
	}
}

func (ob *OrderBook) Close() {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	ob.closed = true
	ob.bids = nil
	ob.asks = nil
	ob.orders = nil
}

var (
	ErrBookClosed      = &BookError{"order book is closed"}
	ErrOrderNotFound   = &BookError{"order not found"}
	ErrInvalidSnapshot = &BookError{"invalid order book snapshot"}
)

type BookError struct {
	message string
}

func (e *BookError) Error() string {
	return e.message
}

func (ob *OrderBook) snapshotStateLocked() orderBookState {
	bids := cloneLevelsFromPointers(ob.bids)
	asks := cloneLevelsFromPointers(ob.asks)
	orders := make([]types.Order, 0, len(ob.orders))
	for _, order := range ob.orders {
		if order == nil {
			continue
		}
		orderCopy := *order
		if order.ExpireAt != nil {
			expireAt := *order.ExpireAt
			orderCopy.ExpireAt = &expireAt
		}
		orders = append(orders, orderCopy)
	}
	sort.Slice(orders, func(i, j int) bool {
		return orders[i].ID < orders[j].ID
	})

	return orderBookState{
		Symbol:    ob.symbol,
		Config:    ob.config,
		Bids:      bids,
		Asks:      asks,
		Orders:    orders,
		Sequence:  ob.sequence,
		UpdatedAt: ob.updatedAt,
	}
}

func encodeSnapshot(body orderBookState) ([]byte, error) {
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(bodyJSON)
	snapshot := orderBookSnapshot{
		Version:  snapshotVersion,
		Checksum: hex.EncodeToString(sum[:]),
		Body:     body,
	}
	return json.MarshalIndent(snapshot, "", "  ")
}

func decodeSnapshot(data []byte) (orderBookState, error) {
	var snapshot orderBookSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return orderBookState{}, err
	}
	if snapshot.Version != snapshotVersion || snapshot.Checksum == "" {
		return orderBookState{}, ErrInvalidSnapshot
	}
	bodyJSON, err := json.Marshal(snapshot.Body)
	if err != nil {
		return orderBookState{}, err
	}
	sum := sha256.Sum256(bodyJSON)
	if snapshot.Checksum != hex.EncodeToString(sum[:]) {
		return orderBookState{}, ErrInvalidSnapshot
	}
	return snapshot.Body, nil
}

func cloneLevels(levels []types.Level) []*types.Level {
	result := make([]*types.Level, 0, len(levels))
	for i := range levels {
		level := levels[i]
		result = append(result, &level)
	}
	return result
}

func cloneLevelsFromPointers(levels []*types.Level) []types.Level {
	result := make([]types.Level, 0, len(levels))
	for _, level := range levels {
		if level != nil {
			result = append(result, *level)
		}
	}
	return result
}

func sortLevels(levels []*types.Level, desc bool) {
	sort.Slice(levels, func(i, j int) bool {
		if desc {
			return levels[i].Price.GreaterThan(levels[j].Price)
		}
		return levels[i].Price.LessThan(levels[j].Price)
	})
}

func insertLevel(levels []*types.Level, level *types.Level, desc bool) []*types.Level {
	levels = append(levels, level)
	sort.Slice(levels, func(i, j int) bool {
		if desc {
			return levels[i].Price.GreaterThan(levels[j].Price)
		}
		return levels[i].Price.LessThan(levels[j].Price)
	})
	return levels
}

func removeLevel(levels []*types.Level, price decimal.Decimal) []*types.Level {
	for i, l := range levels {
		if l.Price.Equal(price) {
			return append(levels[:i], levels[i+1:]...)
		}
	}
	return levels
}
