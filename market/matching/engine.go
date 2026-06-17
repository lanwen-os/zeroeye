package matching

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/tent-of-trials/market/orderbook"
	"github.com/tent-of-trials/market/types"
)

type EngineConfig struct {
	OrderTimeoutMs   int64
	MaxPendingOrders int
	EnableShorting   bool
	FeeRate          string
	MakerFeeRate     string
}

type MatchingEngine struct {
	config     EngineConfig
	books      map[types.Symbol]*orderbook.OrderBook
	trades     []*types.Trade
	tradeCount atomic.Int64
	mu         sync.RWMutex
}

const (
	DefaultOrderBookSnapshotPath     = "data/orderbook_snapshot.json"
	defaultOrderBookSnapshotInterval = 60 * time.Second
	orderBookSnapshotVersion         = 1
)

type orderBookSnapshotFile struct {
	Version  int                   `json:"version"`
	Checksum string                `json:"checksum"`
	Body     orderBookSnapshotBody `json:"body"`
}

type orderBookSnapshotBody struct {
	Books []json.RawMessage `json:"books"`
}

func NewMatchingEngine(config EngineConfig, books map[types.Symbol]*orderbook.OrderBook) *MatchingEngine {
	return &MatchingEngine{
		config: config,
		books:  books,
		trades: make([]*types.Trade, 0, 10000),
	}
}

func SnapshotIntervalFromEnv() time.Duration {
	value := strings.TrimSpace(os.Getenv("OB_SNAPSHOT_INTERVAL_SECS"))
	if value == "" {
		return defaultOrderBookSnapshotInterval
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return defaultOrderBookSnapshotInterval
	}
	return time.Duration(seconds) * time.Second
}

func ChecksumPath(snapshotPath string) string {
	ext := filepath.Ext(snapshotPath)
	if ext == "" {
		return snapshotPath + ".sha256"
	}
	return strings.TrimSuffix(snapshotPath, ext) + ".sha256"
}

func (e *MatchingEngine) SnapshotOrderBooks() ([]byte, error) {
	symbols := make([]string, 0, len(e.books))
	for symbol := range e.books {
		symbols = append(symbols, string(symbol))
	}
	sort.Strings(symbols)

	body := orderBookSnapshotBody{
		Books: make([]json.RawMessage, 0, len(symbols)),
	}
	for _, symbol := range symbols {
		book := e.books[types.Symbol(symbol)]
		if book == nil {
			continue
		}
		data, err := book.Snapshot()
		if err != nil {
			return nil, err
		}
		compact, err := compactJSON(data)
		if err != nil {
			return nil, err
		}
		body.Books = append(body.Books, json.RawMessage(compact))
	}

	bodyJSON, err := marshalSnapshotBody(body)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(bodyJSON)
	snapshot := orderBookSnapshotFile{
		Version:  orderBookSnapshotVersion,
		Checksum: hex.EncodeToString(sum[:]),
		Body:     body,
	}
	return json.MarshalIndent(snapshot, "", "  ")
}

func (e *MatchingEngine) RecoverOrderBooks(data []byte) error {
	body, err := decodeOrderBookSnapshotFile(data)
	if err != nil {
		return err
	}
	for _, bookData := range body.Books {
		symbol, err := orderbook.SnapshotSymbol(bookData)
		if err != nil {
			return err
		}
		book, ok := e.books[symbol]
		if !ok {
			return fmt.Errorf("snapshot contains unknown order book symbol %s", symbol)
		}
		if err := book.Recover(bookData); err != nil {
			return err
		}
	}
	return nil
}

func (e *MatchingEngine) WriteOrderBookSnapshot(path string) error {
	data, err := e.SnapshotOrderBooks()
	if err != nil {
		return err
	}

	var snapshot orderBookSnapshotFile
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := writeFileAtomically(path, data, 0o644); err != nil {
		return err
	}
	return writeFileAtomically(ChecksumPath(path), []byte(snapshot.Checksum+"\n"), 0o644)
}

func (e *MatchingEngine) RecoverOrderBookSnapshot(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	checksumData, err := os.ReadFile(ChecksumPath(path))
	if err != nil {
		return err
	}

	var snapshot orderBookSnapshotFile
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return err
	}
	if strings.TrimSpace(string(checksumData)) != snapshot.Checksum {
		return fmt.Errorf("snapshot checksum file does not match snapshot body")
	}

	return e.RecoverOrderBooks(data)
}

func (e *MatchingEngine) StartOrderBookSnapshots(ctx context.Context, path string, interval time.Duration, onError func(error)) {
	if interval <= 0 {
		interval = defaultOrderBookSnapshotInterval
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := e.WriteOrderBookSnapshot(path); err != nil && onError != nil {
					onError(err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (e *MatchingEngine) PlaceOrder(order *types.Order) ([]*types.Trade, error) {
	if order.ID == "" {
		order.ID = uuid.New().String()
	}
	order.Status = types.New
	order.CreatedAt = time.Now()
	order.UpdatedAt = time.Now()

	book, exists := e.books[order.Symbol]
	if !exists {
		return nil, ErrSymbolNotFound
	}

	trades, err := book.AddOrder(order)
	if err != nil {
		return nil, err
	}

	order.Status = types.Filled
	order.FilledQty = order.Quantity
	order.RemainingQty = decimal.Zero
	order.UpdatedAt = time.Now()

	for _, trade := range trades {
		trade.ID = uuid.New().String()
		trade.Timestamp = time.Now()
		e.mu.Lock()
		e.trades = append(e.trades, trade)
		e.tradeCount.Add(1)
		e.mu.Unlock()
	}

	return trades, nil
}

func (e *MatchingEngine) CancelOrder(symbol types.Symbol, orderID string) error {
	book, exists := e.books[symbol]
	if !exists {
		return ErrSymbolNotFound
	}
	return book.CancelOrder(orderID)
}

func (e *MatchingEngine) GetTradeCount() int64 {
	return e.tradeCount.Load()
}

func (e *MatchingEngine) GetRecentTrades(limit int) []*types.Trade {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 || limit > len(e.trades) {
		limit = len(e.trades)
	}

	result := make([]*types.Trade, limit)
	copy(result, e.trades[len(e.trades)-limit:])
	return result
}

func (e *MatchingEngine) ValidateOrder(order *types.Order) error {
	if order.Quantity.LessThanOrEqual(decimal.Zero) {
		return ErrInvalidQuantity
	}

	if order.Type == types.Limit && order.Price.LessThanOrEqual(decimal.Zero) {
		return ErrInvalidPrice
	}

	if !e.config.EnableShorting && order.Side == types.Sell {
		return ErrShortingDisabled
	}

	return nil
}

var (
	ErrSymbolNotFound   = &EngineError{"symbol not found"}
	ErrInvalidQuantity  = &EngineError{"invalid quantity"}
	ErrInvalidPrice     = &EngineError{"invalid price"}
	ErrShortingDisabled = &EngineError{"shorting disabled"}
)

type EngineError struct {
	message string
}

func (e *EngineError) Error() string {
	return e.message
}

func decodeOrderBookSnapshotFile(data []byte) (orderBookSnapshotBody, error) {
	var snapshot orderBookSnapshotFile
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return orderBookSnapshotBody{}, err
	}
	if snapshot.Version != orderBookSnapshotVersion || snapshot.Checksum == "" {
		return orderBookSnapshotBody{}, fmt.Errorf("invalid order book snapshot file")
	}
	bodyJSON, err := marshalSnapshotBody(snapshot.Body)
	if err != nil {
		return orderBookSnapshotBody{}, err
	}
	sum := sha256.Sum256(bodyJSON)
	if snapshot.Checksum != hex.EncodeToString(sum[:]) {
		return orderBookSnapshotBody{}, fmt.Errorf("order book snapshot checksum mismatch")
	}
	return snapshot.Body, nil
}

func marshalSnapshotBody(body orderBookSnapshotBody) ([]byte, error) {
	normalized := orderBookSnapshotBody{
		Books: make([]json.RawMessage, 0, len(body.Books)),
	}
	for _, book := range body.Books {
		compact, err := compactJSON(book)
		if err != nil {
			return nil, err
		}
		normalized.Books = append(normalized.Books, json.RawMessage(compact))
	}
	return json.Marshal(normalized)
}

func compactJSON(data []byte) ([]byte, error) {
	var value interface{}
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func writeFileAtomically(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}
