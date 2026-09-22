// Command mkqd runs mkq queues as a standalone worker process.
//
//	mkqd run   -c mkqd.yaml    consume the configured queues
//	mkqd check -c mkqd.yaml    validate the config and reach Redis
//	mkqd version               print the build version
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/shiroha-a/mkqd"
)

func main() {
	if err := dispatch(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(2)
		}
		fmt.Fprintf(os.Stderr, "mkqd: %v\n", err)
		os.Exit(1)
	}
}

// command is one subcommand. Keeping the table explicit leaves room for
// the inspect / admin subcommands without pulling in a CLI framework.
type command struct {
	name    string
	summary string
	run     func(args []string) error
}

func commands() []command {
	return []command{
		{"run", "consume the configured queues until interrupted", cmdRun},
		{"check", "validate the config, build executors, ping Redis", cmdCheck},
		{"version", "print the mkqd version", cmdVersion},
	}
}

func dispatch(args []string) error {
	if len(args) == 0 {
		usage()
		return flag.ErrHelp
	}
	switch args[0] {
	case "-h", "--help", "help":
		usage()
		return nil
	}
	for _, c := range commands() {
		if c.name == args[0] {
			return c.run(args[1:])
		}
	}
	usage()
	return fmt.Errorf("unknown command %q", args[0])
}

func usage() {
	var b strings.Builder
	b.WriteString("mkqd — standalone worker for mkq / BullMQ-compatible queues\n\n")
	b.WriteString("usage: mkqd <command> [flags]\n\n")
	for _, c := range commands() {
		fmt.Fprintf(&b, "  %-8s %s\n", c.name, c.summary)
	}
	b.WriteString("\nrun `mkqd <command> -h` for the flags of a command.\n")
	fmt.Fprint(os.Stderr, b.String())
}

// configFlag wires the -c flag shared by every config-driven
// subcommand. MKQD_CONFIG lets a container image set the path once.
func configFlag(fs *flag.FlagSet) *string {
	def := os.Getenv("MKQD_CONFIG")
	if def == "" {
		def = "mkqd.yaml"
	}
	return fs.String("c", def, "path to the mkqd YAML config (env: MKQD_CONFIG)")
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	path := configFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	rt, err := mkqd.NewFromFile(ctx, *path)
	if err != nil {
		return err
	}
	return rt.Run(ctx)
}

func cmdCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	path := configFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx := context.Background()
	rt, err := mkqd.NewFromFile(ctx, *path)
	if err != nil {
		return err
	}
	defer func() { _ = rt.Shutdown(context.Background()) }()

	if err := rt.Check(ctx); err != nil {
		return err
	}

	cfg := rt.Config()
	fmt.Printf("config     %s\n", *path)
	fmt.Printf("redis      %s db=%d pool=%s\n",
		strings.Join(cfg.Redis.Addrs, ","), cfg.Redis.DB, poolDescription(cfg))
	fmt.Printf("prefix     %s\n", keyPrefixDescription(cfg))
	fmt.Printf("server     %s metrics=%t\n", cfg.Server.Addr, cfg.Server.Metrics)
	fmt.Printf("shutdown   %s\n", cfg.ShutdownTimeout.Duration())
	fmt.Println("queues")
	for _, name := range rt.Queues() {
		concurrency, lock, ok := rt.QueueTuning(name)
		if !ok {
			continue
		}
		fmt.Printf("  %-20s concurrency=%d lock=%s\n", name, concurrency, lock)
	}
	fmt.Printf("executors  %s\n", strings.Join(mkqd.RegisteredExecutors(), ", "))
	fmt.Println("ok")
	return nil
}

func poolDescription(cfg mkqd.Config) string {
	if cfg.Redis.PoolSize == 0 {
		return "auto"
	}
	return fmt.Sprint(cfg.Redis.PoolSize)
}

func keyPrefixDescription(cfg mkqd.Config) string {
	if cfg.KeyPrefix == "" {
		return "bull (BullMQ default)"
	}
	return cfg.KeyPrefix
}

func cmdVersion(args []string) error {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	fmt.Println(mkqd.Version)
	return nil
}
