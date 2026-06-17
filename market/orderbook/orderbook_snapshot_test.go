package orderbook

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/tent-of-trials/market/types"
)

func TestSnapshotRecoverRoundTrip(t *testing.T) {
	book := NewOrderBook("BTC-USD", Config{MaxDepth: 100, PriceDecimals: 8, VolumeDecimals: 8})
	createdAt := time.Date(2026, 6, 17, 8, 0, 0, 0, time.UTC)
	expireAt := createdAt.Add(time.Hour)

	_, err := book.AddOrder(&types.Order{
		ID:           "sell-1",
		Symbol:       "BTC-USD",
		Side:         types.Sell,
		Type:         types.Limit,
		Price:        decimal.RequireFromString("65000.50"),
		Quantity:     decimal.RequireFromString("0.25"),
		RemainingQty: decimal.RequireFromString("0.25"),
		ExpireAt:     &expireAt,
	})
	if err != nil {
		t.Fatalf("add sell order: %v", err)
	}
	_, err = book.AddOrder(&types.Order{
		ID:           "buy-1",
		Symbol:       "BTC-USD",
		Side:         types.Buy,
		Type:         types.Limit,
		Price:        decimal.RequireFromString("64000"),
		Quantity:     decimal.RequireFromString("0.5"),
		RemainingQty: decimal.RequireFromString("0.5"),
	})
	if err != nil {
		t.Fatalf("add buy order: %v", err)
	}

	first, err := book.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	second, err := book.Snapshot()
	if err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	if string(first) != string(second) {
		t.Fatal("snapshot output must be deterministic")
	}

	recovered := NewOrderBook("BTC-USD", Config{MaxDepth: 100, PriceDecimals: 8, VolumeDecimals: 8})
	if err := recovered.Recover(first); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got := len(recovered.GetBids()); got != 1 {
		t.Fatalf("recovered bids = %d, want 1", got)
	}
	if got := len(recovered.GetAsks()); got != 1 {
		t.Fatalf("recovered asks = %d, want 1", got)
	}
	if recovered.GetBids()[0].Price.String() != "64000" {
		t.Fatalf("recovered bid price = %s", recovered.GetBids()[0].Price)
	}
}

func TestRecoverRejectsTamperedSnapshot(t *testing.T) {
	book := NewOrderBook("ETH-USD", Config{MaxDepth: 10, PriceDecimals: 8, VolumeDecimals: 8})
	_, err := book.AddOrder(&types.Order{
		ID:           "buy-1",
		Symbol:       "ETH-USD",
		Side:         types.Buy,
		Type:         types.Limit,
		Price:        decimal.RequireFromString("3000"),
		Quantity:     decimal.RequireFromString("1"),
		RemainingQty: decimal.RequireFromString("1"),
	})
	if err != nil {
		t.Fatalf("add order: %v", err)
	}

	data, err := book.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	var envelope map[string]interface{}
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	body := envelope["body"].(map[string]interface{})
	orders := body["orders"].([]interface{})
	order := orders[0].(map[string]interface{})
	order["id"] = "tampered"
	tampered, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal tampered snapshot: %v", err)
	}

	recovered := NewOrderBook("ETH-USD", Config{MaxDepth: 10, PriceDecimals: 8, VolumeDecimals: 8})
	if err := recovered.Recover(tampered); err == nil {
		t.Fatal("Recover accepted a tampered snapshot")
	}
}
