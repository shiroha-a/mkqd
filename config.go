package mkqd

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// Config is the whole mkqd configuration. It is the in-memory shape of
// the YAML file consumed by the mkqd binary, and is equally usable as a
// literal when mkqd is embedded in another Go program.
type Config struct {
	// Redis is forwarded to mkq, which forwards it to go-redis.
	Redis RedisConfig `yaml:"redis"`
	// KeyPrefix maps to BullMQ's "keyPrefix". Empty means "bull",
	// BullMQ's own default.
	KeyPrefix string `yaml:"key_prefix"`
	// Log configures the process logger, which is also handed to mkq
	// as its operational Logger.
	Log LogConfig `yaml:"log"`
	// Server configures the health / metrics HTTP listener.
	Server ServerConfig `yaml:"server"`
	// ShutdownTimeout bounds how long Run waits for in-flight jobs
	// after a termination signal. Zero means 30s.
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
	// Defaults supplies the per-queue tuning used when a queue does
	// not override it.
	Defaults QueueDefaults `yaml:"defaults"`
	// Queues declares the queues this process consumes.
	Queues []QueueConfig `yaml:"queues"`
}

// RedisConfig is the subset of go-redis' UniversalOptions mkqd exposes
// through configuration.
type RedisConfig struct {
	Addrs    []string `yaml:"addrs"`
	DB       int      `yaml:"db"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	// MasterName selects Redis Sentinel mode when non-empty.
	MasterName string `yaml:"master_name"`
	// PoolSize is the go-redis connection pool size. Zero asks mkqd to
	// size it from the declared concurrency (see Config.autoPoolSize).
	PoolSize int `yaml:"pool_size"`
}

// LogConfig configures the slog handler mkqd installs.
type LogConfig struct {
	// Level is one of debug, info, warn, error. Empty means info.
	Level string `yaml:"level"`
	// Format is one of text, json. Empty means text.
	Format string `yaml:"format"`
}

// ServerConfig configures the health / metrics listener.
type ServerConfig struct {
	// Addr is the listen address. Empty means 127.0.0.1:9464; the
	// explicit value "off" disables the listener entirely.
	Addr string `yaml:"addr"`
	// Metrics enables /metrics and wires mkq's Prometheus adapter.
	Metrics bool `yaml:"metrics"`
}

// QueueDefaults is the tuning applied to every queue that does not
// override it.
type QueueDefaults struct {
	Concurrency     int      `yaml:"concurrency"`
	LockDuration    Duration `yaml:"lock_duration"`
	StalledInterval Duration `yaml:"stalled_interval"`
	MaxStalledCount int      `yaml:"max_stalled_count"`
	// JobMetrics enables BullMQ's per-minute metrics HASH when > 0.
	JobMetrics int `yaml:"job_metrics"`
}

// QueueConfig declares one consumed queue. Every tuning field is a
// pointer so that "absent" is distinguishable from "explicitly zero" —
// the distinction drives the precedence rules in queueTuning.merge.
type QueueConfig struct {
	Name            string           `yaml:"name"`
	Concurrency     *int             `yaml:"concurrency"`
	LockDuration    *Duration        `yaml:"lock_duration"`
	StalledInterval *Duration        `yaml:"stalled_interval"`
	MaxStalledCount *int             `yaml:"max_stalled_count"`
	JobMetrics      *int             `yaml:"job_metrics"`
	RateLimit       *RateLimitConfig `yaml:"rate_limit"`
	Executor        *ExecutorConfig  `yaml:"executor"`
}

// RateLimitConfig maps to mkq.WithRateLimit: at most Max jobs per
// Duration window, shared across every worker on the queue.
type RateLimitConfig struct {
	Max      int      `yaml:"max"`
	Duration Duration `yaml:"duration"`
}

// ExecutorConfig names an executor type and carries its type-specific
// options undecoded, so each executor package can decode the rest into
// its own struct without this package knowing the shape.
type ExecutorConfig struct {
	Type string

	node *yaml.Node
}

// UnmarshalYAML captures the executor type and retains the whole node
// for the executor factory to decode.
func (e *ExecutorConfig) UnmarshalYAML(n *yaml.Node) error {
	var head struct {
		Type string `yaml:"type"`
	}
	if err := n.Decode(&head); err != nil {
		return err
	}
	e.Type = head.Type
	// ノードごと保持する。executor 固有のキーは executor 側のパッケージが
	// 自前の構造体へデコードするので、ここでは形を知らなくてよい。
	clone := *n
	e.node = &clone
	return nil
}

// Decode unmarshals the executor-specific options into v. It is the
// entry point an ExecutorFactory uses to read its own configuration.
func (e ExecutorConfig) Decode(v any) error {
	if e.node == nil {
		return nil
	}
	return e.node.Decode(v)
}

// Duration is a time.Duration that unmarshals from a YAML string such
// as "30s" or "5m". Bare numbers are rejected rather than silently
// interpreted as nanoseconds.
type Duration time.Duration

// UnmarshalYAML parses a Go duration string.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a string such as \"30s\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML renders the duration back as a Go duration string.
func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// Duration returns the value as a time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// LoadConfig reads, env-expands, parses and validates a YAML config
// file.
func LoadConfig(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("mkqd: read config: %w", err)
	}
	return ParseConfig(raw)
}

// ParseConfig parses YAML bytes into a validated Config. ${VAR} and
// ${VAR:-default} references are expanded from the process environment
// before parsing; an unset variable without a default is an error so
// that a typo fails loudly instead of producing an empty string.
func ParseConfig(raw []byte) (Config, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return Config{}, fmt.Errorf("mkqd: parse config: %w", err)
	}

	var cfg Config
	// 空ファイルはノードを持たない。展開も検証もするものがないので、
	// 既定値だけを載せて Validate に渡す (addrs 未設定で落ちる)。
	if root.Kind != 0 {
		if err := expandEnvNode(&root); err != nil {
			return Config{}, err
		}
		expanded, err := yaml.Marshal(&root)
		if err != nil {
			return Config{}, fmt.Errorf("mkqd: parse config: %w", err)
		}
		dec := yaml.NewDecoder(bytes.NewReader(expanded))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil {
			return Config{}, fmt.Errorf("mkqd: parse config: %w", err)
		}
	}

	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

var envPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}`)

// expandEnvNode substitutes ${VAR} / ${VAR:-default} in every scalar of
// the parsed document.
//
// **展開はパース後のノードに対して行う。** 生バイト列に対して行うと、
// YAML のコメントに書いた ${VAR} まで置換対象になり、値に YAML の特殊文字が
// 入ると構文が壊れる。ノード単位なら値の中だけに閉じる。
//
// A substituted scalar re-resolves its type, so `db: ${REDIS_DB}`
// yields an integer. Quoting the reference keeps it a string, exactly
// as quoting a literal would: `db: "${REDIS_DB}"` is as invalid as
// `db: "5"`, and `password: "${PW}"` stays a string even when the
// password looks like a number.
func expandEnvNode(n *yaml.Node) error {
	var missing []string
	walkScalars(n, func(s *yaml.Node) {
		if !envPattern.MatchString(s.Value) {
			return
		}
		s.Value = envPattern.ReplaceAllStringFunc(s.Value, func(m string) string {
			g := envPattern.FindStringSubmatch(m)
			if v, ok := os.LookupEnv(g[1]); ok {
				return v
			}
			// 参照が "${VAR:-...}" 形式かどうかは、マッチ全体に ":-" が
			// 含まれるかで判別する。既定値が空文字のケースを取りこぼさない。
			if strings.Contains(m, ":-") {
				return g[2]
			}
			missing = append(missing, g[1])
			return ""
		})
		// 置換後の値で型を解決し直す。YAML は "${VAR}" をまず !!str と
		// 判定しているので、タグを落とさないと db: ${REDIS_DB} が
		// 文字列のままになる。引用符付きの値は Style がそのまま残り、
		// 再シリアライズでも引用符が保たれるため文字列であり続ける。
		s.Tag = ""
	})
	if len(missing) > 0 {
		return fmt.Errorf("mkqd: undefined environment variable(s) in config: %s",
			strings.Join(dedupe(missing), ", "))
	}
	return nil
}

// walkScalars visits every scalar node in the document, including map
// keys — a key is never expected to carry a reference, but skipping it
// would make the traversal state-dependent for no gain.
func walkScalars(n *yaml.Node, fn func(*yaml.Node)) {
	if n == nil {
		return
	}
	if n.Kind == yaml.ScalarNode {
		fn(n)
		return
	}
	for _, c := range n.Content {
		walkScalars(c, fn)
	}
}

func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := in[:0]
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// Built-in tuning defaults. LockDuration / StalledInterval mirror
// BullMQ's own defaults so a shared queue behaves identically no
// matter which runtime picked the job up.
const (
	defaultConcurrency     = 16
	defaultLockDuration    = 30 * time.Second
	defaultStalledInterval = 30 * time.Second
	defaultMaxStalled      = 1
	defaultShutdownTimeout = 30 * time.Second
	defaultServerAddr      = "127.0.0.1:9464"
	// serverOff is the explicit opt-out for the health listener.
	serverOff = "off"
	// poolHeadroom matches mkq's own pool-size warning threshold:
	// BZPopMin holds one connection per worker slot.
	poolHeadroom = 8
)

func (c *Config) applyDefaults() {
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "text"
	}
	if c.Server.Addr == "" {
		c.Server.Addr = defaultServerAddr
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = Duration(defaultShutdownTimeout)
	}
	if c.Defaults.Concurrency == 0 {
		c.Defaults.Concurrency = defaultConcurrency
	}
	if c.Defaults.LockDuration == 0 {
		c.Defaults.LockDuration = Duration(defaultLockDuration)
	}
	if c.Defaults.StalledInterval == 0 {
		c.Defaults.StalledInterval = Duration(defaultStalledInterval)
	}
	if c.Defaults.MaxStalledCount == 0 {
		c.Defaults.MaxStalledCount = defaultMaxStalled
	}
}

// Validate reports configuration errors that would otherwise surface as
// confusing runtime failures.
func (c Config) Validate() error {
	if len(c.Redis.Addrs) == 0 {
		return fmt.Errorf("mkqd: redis.addrs is required")
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("mkqd: log.level %q is not one of debug, info, warn, error", c.Log.Level)
	}
	switch c.Log.Format {
	case "text", "json":
	default:
		return fmt.Errorf("mkqd: log.format %q is not one of text, json", c.Log.Format)
	}
	if c.Defaults.Concurrency < 1 {
		return fmt.Errorf("mkqd: defaults.concurrency must be >= 1, got %d", c.Defaults.Concurrency)
	}
	if c.ShutdownTimeout < 0 {
		return fmt.Errorf("mkqd: shutdown_timeout must not be negative")
	}

	seen := make(map[string]struct{}, len(c.Queues))
	for i, q := range c.Queues {
		if q.Name == "" {
			return fmt.Errorf("mkqd: queues[%d].name is required", i)
		}
		if _, dup := seen[q.Name]; dup {
			return fmt.Errorf("mkqd: queue %q is declared more than once", q.Name)
		}
		seen[q.Name] = struct{}{}

		if q.Concurrency != nil && *q.Concurrency < 1 {
			return fmt.Errorf("mkqd: queues[%s].concurrency must be >= 1, got %d", q.Name, *q.Concurrency)
		}
		if q.RateLimit != nil {
			if q.RateLimit.Max < 1 {
				return fmt.Errorf("mkqd: queues[%s].rate_limit.max must be >= 1", q.Name)
			}
			if q.RateLimit.Duration <= 0 {
				return fmt.Errorf("mkqd: queues[%s].rate_limit.duration must be > 0", q.Name)
			}
		}
		if q.Executor != nil && q.Executor.Type == "" {
			return fmt.Errorf("mkqd: queues[%s].executor.type is required", q.Name)
		}
	}
	return nil
}

// serverEnabled reports whether the health / metrics listener should
// be started.
func (c Config) serverEnabled() bool {
	return c.Server.Addr != "" && c.Server.Addr != serverOff
}

// declaredConcurrency sums the concurrency of every configured queue,
// falling back to the default for queues that do not override it.
func (c Config) declaredConcurrency() int {
	total := 0
	for _, q := range c.Queues {
		if q.Concurrency != nil {
			total += *q.Concurrency
			continue
		}
		total += c.Defaults.Concurrency
	}
	if total == 0 {
		total = c.Defaults.Concurrency
	}
	return total
}

// autoPoolSize is the pool size mkqd picks when redis.pool_size is
// unset.
//
// **mkq は pool が concurrency+8 未満だと起動時に警告を出す。** 単体ワーカーで
// その警告を既定で踏ませたくないので、宣言済み concurrency の合計から自動で
// 算出する。queue を YAML に書かず Go 側だけで登録した場合はここに乗らないため、
// Runtime.Start が不足分を警告する。
func (c Config) autoPoolSize() int {
	return c.declaredConcurrency() + poolHeadroom
}

// queueConfig returns the declared config for name, if any.
func (c Config) queueConfig(name string) (QueueConfig, bool) {
	for _, q := range c.Queues {
		if q.Name == name {
			return q, true
		}
	}
	return QueueConfig{}, false
}
