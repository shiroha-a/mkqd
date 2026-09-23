// Command embedded is the smallest complete mkqd embedding: a typed
// handler, a producer, and the runtime that ties them to Redis.
//
// It is the README's opening snippet as something you can run:
//
//	go run ./examples/embedded
//
// Needs a Redis on 127.0.0.1:6379 (or MKQD_REDIS_ADDR). It uses its
// own key prefix so it cannot disturb a real deployment, and drains
// what it created on the way out.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/shiroha-a/mkq"
	"github.com/shiroha-a/mkqd"
)

// Delivery is the job payload. mkqd is generic over it: the handler
// receives it decoded, and the producer takes it as-is.
type Delivery struct {
	Inbox string `json:"inbox"`
	Body  string `json:"body"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisAddr := os.Getenv("MKQD_REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}

	rt, err := mkqd.New(ctx, mkqd.Config{
		Redis: mkqd.RedisConfig{Addrs: []string{redisAddr}},
		// 実運用のキューに混ざらないよう専用の prefix を使う。
		KeyPrefix: "mkqd-example",
		Log:       mkqd.LogConfig{Level: "info", Format: "text"},
		// このサンプルにヘルスエンドポイントは要らない。
		Server: mkqd.ServerConfig{Addr: "off"},
	})
	if err != nil {
		return err
	}

	var handled atomic.Int64
	if err := mkqd.Handle(rt, "deliver", func(ctx context.Context, job *mkq.Job[Delivery]) (any, error) {
		// 本物のハンドラはここで送信する。ctx は必ず尊重すること:
		// シャットダウンの猶予を使い切ったときだけ cancel される。
		log.Printf("delivering job %s to %s: %s", job.ID, job.Data.Inbox, job.Data.Body)
		handled.Add(1)
		return nil, nil
	}, mkqd.WithConcurrency(4)); err != nil {
		return err
	}

	if err := rt.Start(ctx); err != nil {
		return err
	}

	producer := mkqd.Producer[Delivery](rt, "deliver")
	for i := range 3 {
		job, err := producer.Add(ctx, Delivery{
			Inbox: fmt.Sprintf("https://remote%d.example/inbox", i),
			Body:  "hello",
		})
		if err != nil {
			return err
		}
		log.Printf("enqueued %s", job.ID)
	}

	// 全部処理されるまで少しだけ待つ。実際のアプリはここで HTTP を
	// 提供し続ける (rt.Run が signal を待つ形も使える)。
	deadline := time.Now().Add(10 * time.Second)
	for handled.Load() < 3 && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			// Ctrl-C で抜けてもシャットダウンは下で行う。
			deadline = time.Now()
		case <-time.After(50 * time.Millisecond):
		}
	}
	log.Printf("handled %d job(s)", handled.Load())

	// シャットダウンは親 ctx から切り離す。SIGTERM で cancel された ctx を
	// そのまま渡すと in-flight ジョブの猶予がゼロになる。
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return rt.Shutdown(shutdownCtx)
}
