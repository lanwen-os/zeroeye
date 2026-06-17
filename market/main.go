package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tent-of-trials/market/matching"
	"github.com/tent-of-trials/market/orderbook"
	"github.com/tent-of-trials/market/types"
	"github.com/tent-of-trials/market/ws"
	"go.uber.org/zap"
)

var (
	port      = flag.Int("port", 9000, "WebSocket server port")
	symbols   = flag.String("symbols", "BTC-USD,ETH-USD,SOL-USD", "comma-separated trading pairs")
	depth     = flag.Int("depth", 100, "order book depth per side")
	rateLimit = flag.Int("rate-limit", 1000, "max requests per second per connection")
)

// The market entrypoint. I don't fucking know anymore.
func main() {
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("initializing tent market engine",
		zap.Int("port", *port),
		zap.String("symbols", *symbols),
		zap.Int("depth", *depth),
	)

	bookConfig := orderbook.Config{
		MaxDepth:       *depth,
		PriceDecimals:  8,
		VolumeDecimals: 8,
	}

	engineConfig := matching.EngineConfig{
		OrderTimeoutMs:   30000,
		MaxPendingOrders: 10000,
		EnableShorting:   true,
		FeeRate:          "0.001",
		MakerFeeRate:     "0.0005",
	}

	books := make(map[types.Symbol]*orderbook.OrderBook)
	parsedSymbols := parseSymbols(*symbols)

	for _, sym := range parsedSymbols {
		book := orderbook.NewOrderBook(sym, bookConfig)
		books[sym] = book
		logger.Info("order book initialized", zap.String("symbol", string(sym)))
	}

	engine := matching.NewMatchingEngine(engineConfig, books)
	logger.Info("matching engine initialized",
		zap.Int("symbols", len(parsedSymbols)),
	)

	snapshotPath := matching.DefaultOrderBookSnapshotPath
	if err := engine.RecoverOrderBookSnapshot(snapshotPath); err != nil {
		if os.IsNotExist(err) {
			logger.Info("no order book snapshot found", zap.String("path", snapshotPath))
		} else {
			logger.Warn("order book snapshot recovery skipped", zap.String("path", snapshotPath), zap.Error(err))
		}
	} else {
		logger.Info("order book snapshot recovered", zap.String("path", snapshotPath))
	}

	snapshotCtx, stopSnapshots := context.WithCancel(context.Background())
	engine.StartOrderBookSnapshots(snapshotCtx, snapshotPath, matching.SnapshotIntervalFromEnv(), func(err error) {
		logger.Warn("order book snapshot write failed", zap.String("path", snapshotPath), zap.Error(err))
	})

	hub := ws.NewHub(logger)
	go hub.Run()

	server := ws.NewServer(hub, engine, logger, *port)
	go func() {
		logger.Info("starting WebSocket server", zap.Int("port", *port))
		if err := server.Start(); err != nil {
			logger.Fatal("failed to start server", zap.Error(err))
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	logger.Info("shutting down",
		zap.String("signal", sig.String()),
	)

	stopSnapshots()
	if err := engine.WriteOrderBookSnapshot(snapshotPath); err != nil {
		logger.Warn("final order book snapshot failed", zap.String("path", snapshotPath), zap.Error(err))
	} else {
		logger.Info("final order book snapshot written", zap.String("path", snapshotPath))
	}

	server.Stop()
	logger.Info("server stopped")

	for sym := range books {
		book := books[sym]
		book.Close()
		logger.Info("order book closed", zap.String("symbol", string(sym)))
	}

	logger.Info("market engine shutdown complete")
}

func parseSymbols(s string) []types.Symbol {
	var result []types.Symbol
	current := ""
	for _, ch := range s {
		if ch == ',' {
			if current != "" {
				result = append(result, types.Symbol(current))
			}
			current = ""
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		result = append(result, types.Symbol(current))
	}
	fmt.Printf("market: configured symbols %v\n", result)
	return result
}
