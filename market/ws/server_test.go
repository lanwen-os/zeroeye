package ws

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/tent-of-trials/market/matching"
	"github.com/tent-of-trials/market/orderbook"
	"github.com/tent-of-trials/market/types"
	"go.uber.org/zap"
)

func TestHandleOrderBookSnapshotWritesSnapshotAndChecksum(t *testing.T) {
	t.Chdir(t.TempDir())

	book := orderbook.NewOrderBook("BTC-USD", orderbook.Config{MaxDepth: 100, PriceDecimals: 8, VolumeDecimals: 8})
	_, err := book.AddOrder(&types.Order{
		ID:           "buy-1",
		Symbol:       "BTC-USD",
		Side:         types.Buy,
		Type:         types.Limit,
		Price:        decimal.RequireFromString("64000"),
		Quantity:     decimal.RequireFromString("0.5"),
		RemainingQty: decimal.RequireFromString("0.5"),
	})
	if err != nil {
		t.Fatalf("add order: %v", err)
	}

	books := map[types.Symbol]*orderbook.OrderBook{"BTC-USD": book}
	engine := matching.NewMatchingEngine(matching.EngineConfig{}, books)
	server := NewServer(nil, engine, zap.NewNop(), 0)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/admin/orderbook/snapshot", nil)
	server.handleOrderBookSnapshot(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["status"] != "ok" {
		t.Fatalf("status response = %q, want ok", response["status"])
	}
	if response["snapshot"] != matching.DefaultOrderBookSnapshotPath {
		t.Fatalf("snapshot path = %q, want %q", response["snapshot"], matching.DefaultOrderBookSnapshotPath)
	}
	if response["checksum"] != matching.ChecksumPath(matching.DefaultOrderBookSnapshotPath) {
		t.Fatalf("checksum path = %q, want %q", response["checksum"], matching.ChecksumPath(matching.DefaultOrderBookSnapshotPath))
	}

	snapshotData, err := os.ReadFile(response["snapshot"])
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	checksumData, err := os.ReadFile(response["checksum"])
	if err != nil {
		t.Fatalf("read checksum: %v", err)
	}

	var snapshot struct {
		Checksum string `json:"checksum"`
		Body     struct {
			Books []json.RawMessage `json:"books"`
		} `json:"body"`
	}
	if err := json.Unmarshal(snapshotData, &snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snapshot.Checksum == "" {
		t.Fatal("snapshot checksum is empty")
	}
	if len(snapshot.Body.Books) != 1 {
		t.Fatalf("snapshot book count = %d, want 1", len(snapshot.Body.Books))
	}
	if strings.TrimSpace(string(checksumData)) != snapshot.Checksum {
		t.Fatalf("checksum file = %q, want %q", strings.TrimSpace(string(checksumData)), snapshot.Checksum)
	}
}

func TestHandleOrderBookSnapshotRequiresPost(t *testing.T) {
	t.Chdir(t.TempDir())

	engine := matching.NewMatchingEngine(matching.EngineConfig{}, nil)
	server := NewServer(nil, engine, zap.NewNop(), 0)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/admin/orderbook/snapshot", nil)
	server.handleOrderBookSnapshot(recorder, request)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
	if _, err := os.Stat(matching.DefaultOrderBookSnapshotPath); !os.IsNotExist(err) {
		t.Fatalf("snapshot file should not be written for GET, stat error: %v", err)
	}
}
