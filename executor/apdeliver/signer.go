package apdeliver

import (
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/shiroha-a/mkqd"
	"github.com/shiroha-a/mkqd/httpsig"
)

// Config is the YAML block of the "activitypub_deliver" executor.
type Config struct {
	Timeout             mkqd.Duration `yaml:"timeout"`
	UserAgent           string        `yaml:"user_agent"`
	MaxResponseBytes    int64         `yaml:"max_response_bytes"`
	AllowPrivateNetwork bool          `yaml:"allow_private_network"`
	Signer              SignerConfig  `yaml:"signer"`
}

// SignerConfig selects how a standalone mkqd reaches a signing key.
//
// An embedding application does not use this: it passes a Signer to New
// and its keys never leave the process. The configured signers are for
// deployments where mkqd runs on its own — and "remote" keeps that
// property there too, by asking the application to sign rather than
// asking it for the key.
type SignerConfig struct {
	// Type is "file", "dir" or "remote".
	Type string `yaml:"type"`
	// KeyID is the key id a "file" signer advertises. Required there.
	KeyID string `yaml:"key_id"`
	// PrivateKeyPath is the PEM a "file" signer loads.
	PrivateKeyPath string `yaml:"private_key_path"`
	// Path is the directory a "dir" signer reads keys from.
	Path string `yaml:"path"`
	// URL is the signing endpoint a "remote" signer posts to.
	URL string `yaml:"url"`
	// Secret keys the HMAC that authenticates mkqd to that endpoint.
	// Empty sends no signature and is logged as a warning: an
	// unauthenticated signing endpoint will sign anything for anyone
	// who can reach it.
	Secret string `yaml:"secret"`
	// Timeout bounds one signing call. Zero means 5s.
	Timeout mkqd.Duration `yaml:"timeout"`
	// AllowPrivateNetwork defaults to true for "remote": the endpoint
	// is the operator's own and loopback is the normal case.
	AllowPrivateNetwork *bool `yaml:"allow_private_network"`
}

func init() {
	mkqd.RegisterExecutor("activitypub_deliver", newExecutor)
}

func newExecutor(_ context.Context, bc mkqd.BuildContext, raw mkqd.ExecutorConfig) (mkqd.Executor, error) {
	var cfg Config
	if err := raw.DecodeStrict(&cfg); err != nil {
		return nil, err
	}
	signer, err := BuildSigner(cfg.Signer)
	if err != nil {
		return nil, fmt.Errorf("executor activitypub_deliver: %w", err)
	}
	// 署名エンドポイントに認証が無いと、そこへ到達できるものは誰でも任意の
	// バイト列に任意のアクターの鍵で署名させられる。loopback 限定の構成なら
	// 成り立つが、黙って通すには重すぎるので必ず知らせる。
	if cfg.Signer.Type == "remote" && cfg.Signer.Secret == "" {
		bc.Logger.Warn("remote signer has no secret; anything that can reach the endpoint can have arbitrary bytes signed",
			"url", cfg.Signer.URL)
	}
	return New(Options{
		Signer:              signer,
		Timeout:             cfg.Timeout.Duration(),
		UserAgent:           cfg.UserAgent,
		MaxResponseBytes:    cfg.MaxResponseBytes,
		AllowPrivateNetwork: cfg.AllowPrivateNetwork,
		Logger:              bc.Logger,
	})
}

// BuildSigner constructs a signer from configuration.
func BuildSigner(cfg SignerConfig) (httpsig.Signer, error) {
	switch cfg.Type {
	case "file":
		return newFileSigner(cfg)
	case "dir":
		return newDirSigner(cfg)
	case "remote":
		return newRemoteSigner(cfg)
	case "":
		return nil, errors.New("signer.type is required (file, dir or remote)")
	default:
		return nil, fmt.Errorf("unknown signer type %q (known: file, dir, remote)", cfg.Type)
	}
}

func newFileSigner(cfg SignerConfig) (httpsig.Signer, error) {
	if cfg.KeyID == "" {
		return nil, errors.New("signer.key_id is required for a file signer")
	}
	if cfg.PrivateKeyPath == "" {
		return nil, errors.New("signer.private_key_path is required for a file signer")
	}
	pemBytes, err := os.ReadFile(cfg.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", cfg.PrivateKeyPath, err)
	}
	key, err := httpsig.ParsePrivateKey(pemBytes)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", cfg.PrivateKeyPath, err)
	}
	return httpsig.NewLocalSigner(cfg.KeyID, key)
}

// KeyFileName is where a "dir" signer looks for the key of a key id.
//
// **鍵 ID は URL なので、そのままファイル名にできない。** ハッシュにすると
// 一意で衝突せず長さも揃うが、人間が対応を追えなくなるので
// `mkqd keys path <keyId>` で引けるようにしてある。
func KeyFileName(keyID string) string {
	sum := sha256.Sum256([]byte(keyID))
	return hex.EncodeToString(sum[:]) + ".pem"
}

// dirSigner serves per-actor keys from a directory, caching what it has
// parsed. A multi-user deployment that can export its keys to disk uses
// this; one that cannot should keep its keys and supply a Signer.
type dirSigner struct {
	dir string

	mu     sync.RWMutex
	cached map[string]cachedKey
}

// cachedKey remembers what the file looked like when it was parsed.
//
// **鍵の差し替えは keyId を変えずに行われる。** ActivityPub の鍵ローテーション
// は `https://host/users/x#main-key` という ID を保ったまま鍵材だけ入れ替える
// のが普通なので、内容を見ずにキャッシュし続けると、ローテーション後は古い鍵で
// 署名し続けて全配送が 401 になる。stat 1 回は配送の HTTP POST に比べれば
// 無視できるコストなので、毎回見に行く。
type cachedKey struct {
	key     *rsa.PrivateKey
	modTime time.Time
	size    int64
}

func newDirSigner(cfg SignerConfig) (httpsig.Signer, error) {
	if cfg.Path == "" {
		return nil, errors.New("signer.path is required for a dir signer")
	}
	info, err := os.Stat(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("stat %s: %w", cfg.Path, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", cfg.Path)
	}
	return &dirSigner{dir: cfg.Path, cached: map[string]cachedKey{}}, nil
}

// Sign implements httpsig.Signer.
func (d *dirSigner) Sign(_ context.Context, keyID string, signingString []byte) (httpsig.Signature, error) {
	if keyID == "" {
		return httpsig.Signature{}, fmt.Errorf("%w: a dir signer has no default key", httpsig.ErrUnknownKey)
	}
	key, err := d.load(keyID)
	if err != nil {
		return httpsig.Signature{}, err
	}
	local, err := httpsig.NewLocalSigner(keyID, key)
	if err != nil {
		return httpsig.Signature{}, err
	}
	return local.Sign(context.Background(), keyID, signingString)
}

func (d *dirSigner) load(keyID string) (*rsa.PrivateKey, error) {
	path := filepath.Join(d.dir, KeyFileName(keyID))
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s (expected %s)", httpsig.ErrUnknownKey, keyID, path)
		}
		return nil, fmt.Errorf("apdeliver: stat %s: %w", path, err)
	}

	d.mu.RLock()
	hit, ok := d.cached[keyID]
	d.mu.RUnlock()
	if ok && hit.modTime.Equal(info.ModTime()) && hit.size == info.Size() {
		return hit.key, nil
	}

	pemBytes, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s (expected %s)", httpsig.ErrUnknownKey, keyID, path)
		}
		return nil, fmt.Errorf("apdeliver: read %s: %w", path, err)
	}
	key, err := httpsig.ParsePrivateKey(pemBytes)
	if err != nil {
		return nil, fmt.Errorf("apdeliver: %s: %w", path, err)
	}

	d.mu.Lock()
	d.cached[keyID] = cachedKey{key: key, modTime: info.ModTime(), size: info.Size()}
	d.mu.Unlock()
	return key, nil
}
