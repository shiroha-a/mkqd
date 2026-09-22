package mkqd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const minimalYAML = `
redis:
  addrs: ["127.0.0.1:6379"]
queues:
  - name: email
    executor:
      type: log
`

func TestParseConfig_AppliesDefaults(t *testing.T) {
	cfg, err := ParseConfig([]byte(minimalYAML))
	require.NoError(t, err)

	require.Equal(t, "info", cfg.Log.Level)
	require.Equal(t, "text", cfg.Log.Format)
	require.Equal(t, defaultServerAddr, cfg.Server.Addr)
	require.Equal(t, 30*time.Second, cfg.ShutdownTimeout.Duration())
	require.Equal(t, defaultConcurrency, cfg.Defaults.Concurrency)
	require.Equal(t, 30*time.Second, cfg.Defaults.LockDuration.Duration())
	require.Equal(t, 30*time.Second, cfg.Defaults.StalledInterval.Duration())
	require.Equal(t, 1, cfg.Defaults.MaxStalledCount)
}

func TestParseConfig_DurationsAreStrings(t *testing.T) {
	cfg, err := ParseConfig([]byte(`
redis:
  addrs: ["127.0.0.1:6379"]
shutdown_timeout: 90s
defaults:
  lock_duration: 1m30s
queues:
  - name: q
    rate_limit: { max: 10, duration: 250ms }
    executor: { type: log }
`))
	require.NoError(t, err)
	require.Equal(t, 90*time.Second, cfg.ShutdownTimeout.Duration())
	require.Equal(t, 90*time.Second, cfg.Defaults.LockDuration.Duration())
	require.Equal(t, 250*time.Millisecond, cfg.Queues[0].RateLimit.Duration.Duration())
}

func TestParseConfig_RejectsBareNumberDuration(t *testing.T) {
	_, err := ParseConfig([]byte(`
redis:
  addrs: ["127.0.0.1:6379"]
shutdown_timeout: 30
queues:
  - name: q
    executor: { type: log }
`))
	require.Error(t, err)
	// YAML のスカラ 30 は文字列 "30" として読めてしまうので、単位の欠落を
	// time.ParseDuration 側で捕まえる。どちらの経路でも拒否されることが要点。
	require.Contains(t, err.Error(), `invalid duration "30"`)
}

func TestParseConfig_RejectsNonScalarDuration(t *testing.T) {
	_, err := ParseConfig([]byte(`
redis:
  addrs: ["127.0.0.1:6379"]
shutdown_timeout: { seconds: 30 }
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duration must be a string")
}

func TestParseConfig_RejectsUnknownField(t *testing.T) {
	_, err := ParseConfig([]byte(`
redis:
  addrs: ["127.0.0.1:6379"]
concurency: 4
queues:
  - name: q
    executor: { type: log }
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "concurency")
}

func TestExpandEnv_SubstitutesAndDefaults(t *testing.T) {
	t.Setenv("MKQD_TEST_PASSWORD", "s3cret")

	cfg, err := ParseConfig([]byte(`
redis:
  addrs: ["127.0.0.1:6379"]
  password: "${MKQD_TEST_PASSWORD}"
  username: "${MKQD_TEST_UNSET_USER:-default-user}"
queues:
  - name: q
    executor: { type: log }
`))
	require.NoError(t, err)
	require.Equal(t, "s3cret", cfg.Redis.Password)
	require.Equal(t, "default-user", cfg.Redis.Username)
}

func TestExpandEnv_EmptyDefaultIsHonoured(t *testing.T) {
	// ${VAR:-} は「未定義なら空文字」。未定義エラーに落ちてはいけない。
	cfg, err := ParseConfig([]byte(`
redis:
  addrs: ["127.0.0.1:6379"]
  password: "${MKQD_TEST_STILL_UNSET:-}"
queues:
  - name: q
    executor: { type: log }
`))
	require.NoError(t, err)
	require.Equal(t, "", cfg.Redis.Password)
}

func TestExpandEnv_UndefinedVariableIsAnError(t *testing.T) {
	_, err := ParseConfig([]byte(`
redis:
  addrs: ["${MKQD_TEST_NO_SUCH_VAR}"]
queues:
  - name: q
    executor: { type: log }
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "MKQD_TEST_NO_SUCH_VAR")
}

func TestExpandEnv_ReportsEachMissingNameOnce(t *testing.T) {
	_, err := ParseConfig([]byte(`
redis:
  addrs: ["${MKQD_TEST_MISSING_A}", "${MKQD_TEST_MISSING_A}", "${MKQD_TEST_MISSING_B}"]
`))
	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, "MKQD_TEST_MISSING_A")
	require.Contains(t, msg, "MKQD_TEST_MISSING_B")
	// 同じ名前が 3 箇所で参照されていても、報告は 1 回にまとめる。
	require.Equal(t, 1, strings.Count(msg, "MKQD_TEST_MISSING_A"))
}

func TestValidate_Errors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "missing addrs",
			yaml: "queues:\n  - name: q\n    executor: { type: log }\n",
			want: "redis.addrs is required",
		},
		{
			name: "duplicate queue",
			yaml: "redis:\n  addrs: [\"x:1\"]\nqueues:\n  - name: q\n    executor: { type: log }\n  - name: q\n    executor: { type: log }\n",
			want: "declared more than once",
		},
		{
			name: "empty queue name",
			yaml: "redis:\n  addrs: [\"x:1\"]\nqueues:\n  - executor: { type: log }\n",
			want: "queues[0].name is required",
		},
		{
			name: "bad log level",
			yaml: "redis:\n  addrs: [\"x:1\"]\nlog:\n  level: verbose\n",
			want: "log.level",
		},
		{
			name: "bad log format",
			yaml: "redis:\n  addrs: [\"x:1\"]\nlog:\n  format: logfmt\n",
			want: "log.format",
		},
		{
			name: "zero queue concurrency",
			yaml: "redis:\n  addrs: [\"x:1\"]\nqueues:\n  - name: q\n    concurrency: 0\n    executor: { type: log }\n",
			want: "concurrency must be >= 1",
		},
		{
			name: "rate limit without max",
			yaml: "redis:\n  addrs: [\"x:1\"]\nqueues:\n  - name: q\n    rate_limit: { max: 0, duration: 1s }\n    executor: { type: log }\n",
			want: "rate_limit.max must be >= 1",
		},
		{
			name: "rate limit without duration",
			yaml: "redis:\n  addrs: [\"x:1\"]\nqueues:\n  - name: q\n    rate_limit: { max: 5, duration: 0s }\n    executor: { type: log }\n",
			want: "rate_limit.duration must be > 0",
		},
		{
			name: "executor without type",
			yaml: "redis:\n  addrs: [\"x:1\"]\nqueues:\n  - name: q\n    executor: { url: http://x }\n",
			want: "executor.type is required",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseConfig([]byte(tc.yaml))
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestExecutorConfig_DecodeKeepsTypeSpecificFields(t *testing.T) {
	cfg, err := ParseConfig([]byte(`
redis:
  addrs: ["127.0.0.1:6379"]
queues:
  - name: inbox
    executor:
      type: http
      url: "http://127.0.0.1:3000/_mkqd/jobs"
      timeout: 45s
`))
	require.NoError(t, err)

	var opts struct {
		URL     string   `yaml:"url"`
		Timeout Duration `yaml:"timeout"`
	}
	require.NoError(t, cfg.Queues[0].Executor.Decode(&opts))
	require.Equal(t, "http://127.0.0.1:3000/_mkqd/jobs", opts.URL)
	require.Equal(t, 45*time.Second, opts.Timeout.Duration())
}

func TestAutoPoolSize_SumsDeclaredConcurrency(t *testing.T) {
	cfg, err := ParseConfig([]byte(`
redis:
  addrs: ["127.0.0.1:6379"]
defaults:
  concurrency: 4
queues:
  - name: a
    concurrency: 32
    executor: { type: log }
  - name: b
    executor: { type: log }
`))
	require.NoError(t, err)
	// 32 (explicit) + 4 (defaults) + 8 headroom
	require.Equal(t, 44, cfg.autoPoolSize())
}

func TestAutoPoolSize_NoQueuesFallsBackToDefaults(t *testing.T) {
	cfg, err := ParseConfig([]byte("redis:\n  addrs: [\"x:1\"]\ndefaults:\n  concurrency: 6\n"))
	require.NoError(t, err)
	require.Equal(t, 14, cfg.autoPoolSize())
}

func TestServerEnabled_OffDisablesListener(t *testing.T) {
	cfg, err := ParseConfig([]byte("redis:\n  addrs: [\"x:1\"]\nserver:\n  addr: \"off\"\n"))
	require.NoError(t, err)
	require.False(t, cfg.serverEnabled())

	cfg, err = ParseConfig([]byte("redis:\n  addrs: [\"x:1\"]\n"))
	require.NoError(t, err)
	require.True(t, cfg.serverEnabled())
}

func TestLoadConfig_ReadsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mkqd.yaml")
	require.NoError(t, os.WriteFile(path, []byte(minimalYAML), 0o600))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	require.Equal(t, []string{"127.0.0.1:6379"}, cfg.Redis.Addrs)
	require.Len(t, cfg.Queues, 1)
	require.Equal(t, "log", cfg.Queues[0].Executor.Type)
}

func TestExpandEnv_IgnoresComments(t *testing.T) {
	// コメント内の ${VAR} は展開対象にしない。参照ドキュメントを設定ファイルの
	// コメントとして同梱できることが要件。
	cfg, err := ParseConfig([]byte(`
# ${MKQD_TEST_COMMENT_ONLY} and ${OTHER:-x} appear only in this comment.
redis:
  addrs: ["127.0.0.1:6379"] # trailing ${ALSO_A_COMMENT}
`))
	require.NoError(t, err)
	require.Equal(t, []string{"127.0.0.1:6379"}, cfg.Redis.Addrs)
}

func TestExpandEnv_UnquotedScalarReResolvesType(t *testing.T) {
	t.Setenv("MKQD_TEST_DB", "5")
	t.Setenv("MKQD_TEST_METRICS", "true")

	cfg, err := ParseConfig([]byte(`
redis:
  addrs: ["127.0.0.1:6379"]
  db: ${MKQD_TEST_DB}
server:
  metrics: ${MKQD_TEST_METRICS}
`))
	require.NoError(t, err)
	require.Equal(t, 5, cfg.Redis.DB)
	require.True(t, cfg.Server.Metrics)
}

func TestExpandEnv_QuotedScalarStaysAString(t *testing.T) {
	// 引用符を付けた参照は文字列のまま。これは「${VAR} はそこにリテラルを
	// 書いたのと同じ」という規則の裏返しで、db: "5" が int に入らないのと
	// 同様に db: "${VAR}" も int フィールドには入らない。
	t.Setenv("MKQD_TEST_DB", "5")

	_, err := ParseConfig([]byte(`
redis:
  addrs: ["127.0.0.1:6379"]
  db: "${MKQD_TEST_DB}"
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "!!str")

	// 引用符なしなら数値として解決する。
	cfg, err := ParseConfig([]byte(`
redis:
  addrs: ["127.0.0.1:6379"]
  db: ${MKQD_TEST_DB}
`))
	require.NoError(t, err)
	require.Equal(t, 5, cfg.Redis.DB)
}

func TestExpandEnv_SpecialCharactersDoNotBreakParsing(t *testing.T) {
	// 生バイト列を置換していると、この値で YAML が壊れる。
	t.Setenv("MKQD_TEST_GNARLY", `a: b #c "d" {e}`)

	cfg, err := ParseConfig([]byte(`
redis:
  addrs: ["127.0.0.1:6379"]
  password: "${MKQD_TEST_GNARLY}"
`))
	require.NoError(t, err)
	require.Equal(t, `a: b #c "d" {e}`, cfg.Redis.Password)
}

func TestParseConfig_EmptyDocument(t *testing.T) {
	_, err := ParseConfig(nil)
	require.ErrorContains(t, err, "redis.addrs is required")
}
