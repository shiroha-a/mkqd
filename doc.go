// Package mkqd turns the mkq job-queue library into a runnable worker.
//
// It exists in two shapes that share one implementation:
//
//   - the mkqd binary, driven entirely by a YAML file, which consumes
//     queues without any application code; and
//   - this package, embedded in a Go application that registers typed
//     handlers and calls Run.
//
// Because mkq is wire-compatible with BullMQ v5+, either shape can
// share a queue with BullMQ workers in other languages and with
// dashboards such as bull-board.
//
// # Embedding
//
//	rt, err := mkqd.NewFromFile(ctx, "mkqd.yaml")
//	if err != nil { ... }
//
//	mkqd.Handle(rt, "inbox", func(ctx context.Context, job *mkq.Job[Inbox]) (any, error) {
//	    return nil, process(ctx, job.Data)
//	})
//
//	if err := rt.Run(ctx); err != nil { ... }
//
// Run blocks until the context is cancelled or the process receives
// SIGINT / SIGTERM, then drains in-flight jobs within
// shutdown_timeout.
//
// # Tuning precedence
//
// A queue's runtime tuning is resolved in three layers, later winning
// over earlier:
//
//  1. built-in defaults, then the config file's defaults block;
//  2. the QueueOptions passed at registration in Go;
//  3. the queue's own block in the config file.
//
// Execution, by contrast, always belongs to code: when a queue has both
// a handler registered through Handle and an executor declared in YAML,
// the handler wins and the executor declaration is logged as ignored.
// The split keeps behaviour with the developer and tuning with the
// operator, so concurrency can change without a rebuild.
package mkqd
