package analytics

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestStartIdempotent(t *testing.T) {
	c := NewCollector().WithBatchSize(10).WithFlushInterval(10 * time.Millisecond).WithMaxBacklog(100)
	defer c.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Start(ctx)
		}()
	}
	wg.Wait()

	for i := 0; i < 50; i++ {
		c.Record(MetricSample{Name: "test", Value: float64(i), Type: MetricTypeGauge})
	}

	time.Sleep(50 * time.Millisecond)

	stats := c.Stats()
	if stats.FlushedSamples == 0 {
		t.Errorf("expected samples to be flushed, but FlushedSamples = 0")
	}

	c.Stop()
}

func TestStartAfterStop(t *testing.T) {
	c := NewCollector().WithBatchSize(10).WithFlushInterval(10 * time.Millisecond).WithMaxBacklog(100)

	ctx, cancel := context.WithCancel(context.Background())

	c.Start(ctx)

	for i := 0; i < 10; i++ {
		c.Record(MetricSample{Name: "test", Value: float64(i), Type: MetricTypeGauge})
	}

	time.Sleep(30 * time.Millisecond)

	cancel()
	time.Sleep(10 * time.Millisecond)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	c.Start(ctx2)

	for i := 0; i < 10; i++ {
		c.Record(MetricSample{Name: "test", Value: float64(i), Type: MetricTypeGauge})
	}

	time.Sleep(30 * time.Millisecond)

	stats := c.Stats()
	if stats.FlushedSamples < 10 {
		t.Errorf("expected at least 10 flushed samples after restart, got %d", stats.FlushedSamples)
	}
}
