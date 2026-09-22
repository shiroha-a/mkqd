package apdeliver

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/shiroha-a/mkqd"
	"github.com/shiroha-a/mkqd/httpsig"
)

func writeKey(t *testing.T, path string) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(block), 0o600))
	return key
}

func TestFileSigner_SignsWithTheConfiguredKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "actor.pem")
	key := writeKey(t, path)

	signer, err := BuildSigner(SignerConfig{Type: "file", KeyID: testKeyID, PrivateKeyPath: path})
	require.NoError(t, err)

	sig, err := signer.Sign(context.Background(), "", []byte("signing string"))
	require.NoError(t, err)
	require.Equal(t, testKeyID, sig.KeyID)
	require.Equal(t, httpsig.AlgorithmRSASHA256, sig.Algorithm)

	// 生成した鍵と同じもので署名されていることを、公開鍵で確かめる。
	local, err := httpsig.NewLocalSigner(testKeyID, key)
	require.NoError(t, err)
	want, err := local.Sign(context.Background(), "", []byte("signing string"))
	require.NoError(t, err)
	require.Equal(t, want.Bytes, sig.Bytes)
}

func TestFileSigner_ConfigErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "actor.pem")
	writeKey(t, path)

	_, err := BuildSigner(SignerConfig{Type: "file", PrivateKeyPath: path})
	require.ErrorContains(t, err, "key_id is required")

	_, err = BuildSigner(SignerConfig{Type: "file", KeyID: testKeyID})
	require.ErrorContains(t, err, "private_key_path is required")

	_, err = BuildSigner(SignerConfig{Type: "file", KeyID: testKeyID, PrivateKeyPath: filepath.Join(dir, "nope.pem")})
	require.ErrorContains(t, err, "read")

	junk := filepath.Join(dir, "junk.pem")
	require.NoError(t, os.WriteFile(junk, []byte("not a key"), 0o600))
	_, err = BuildSigner(SignerConfig{Type: "file", KeyID: testKeyID, PrivateKeyPath: junk})
	require.ErrorContains(t, err, "no PEM block")
}

func TestKeyFileName_IsTheHashOfTheKeyID(t *testing.T) {
	sum := sha256.Sum256([]byte(testKeyID))
	require.Equal(t, hex.EncodeToString(sum[:])+".pem", KeyFileName(testKeyID))

	// 鍵 ID が違えばファイル名も違う。
	require.NotEqual(t, KeyFileName("a"), KeyFileName("b"))
}

func TestDirSigner_ResolvesPerKey(t *testing.T) {
	dir := t.TempDir()
	alice := "https://local.example/users/alice#main-key"
	bob := "https://local.example/users/bob#main-key"
	aliceKey := writeKey(t, filepath.Join(dir, KeyFileName(alice)))
	bobKey := writeKey(t, filepath.Join(dir, KeyFileName(bob)))

	signer, err := BuildSigner(SignerConfig{Type: "dir", Path: dir})
	require.NoError(t, err)

	for keyID, key := range map[string]*rsa.PrivateKey{alice: aliceKey, bob: bobKey} {
		sig, err := signer.Sign(context.Background(), keyID, []byte("msg"))
		require.NoError(t, err)
		require.Equal(t, keyID, sig.KeyID)

		local, err := httpsig.NewLocalSigner(keyID, key)
		require.NoError(t, err)
		want, err := local.Sign(context.Background(), keyID, []byte("msg"))
		require.NoError(t, err)
		require.Equal(t, want.Bytes, sig.Bytes, "each key id must sign with its own key")
	}
}

func TestDirSigner_MissingKeyIsUnknown(t *testing.T) {
	signer, err := BuildSigner(SignerConfig{Type: "dir", Path: t.TempDir()})
	require.NoError(t, err)

	_, err = signer.Sign(context.Background(), "https://local.example/users/nobody#main-key", []byte("msg"))
	require.ErrorIs(t, err, httpsig.ErrUnknownKey)
}

func TestDirSigner_HasNoDefaultKey(t *testing.T) {
	// 単一アクターではないので、keyId を省略されても代わりに使う鍵がない。
	signer, err := BuildSigner(SignerConfig{Type: "dir", Path: t.TempDir()})
	require.NoError(t, err)

	_, err = signer.Sign(context.Background(), "", []byte("msg"))
	require.ErrorIs(t, err, httpsig.ErrUnknownKey)
	require.ErrorContains(t, err, "no default key")
}

func TestDirSigner_ReusesAParsedKey(t *testing.T) {
	dir := t.TempDir()
	keyID := "https://local.example/users/alice#main-key"
	path := filepath.Join(dir, KeyFileName(keyID))
	writeKey(t, path)

	signer, err := BuildSigner(SignerConfig{Type: "dir", Path: dir})
	require.NoError(t, err)

	first, err := signer.Sign(context.Background(), keyID, []byte("msg"))
	require.NoError(t, err)
	second, err := signer.Sign(context.Background(), keyID, []byte("msg"))
	require.NoError(t, err)
	require.Equal(t, first.Bytes, second.Bytes)
}

// ActivityPub key rotation replaces the key material while keeping the
// key id, so a cache with no invalidation would keep signing with the
// old key and every delivery for that actor would start failing 401.
func TestDirSigner_PicksUpARotatedKey(t *testing.T) {
	dir := t.TempDir()
	keyID := "https://local.example/users/alice#main-key"
	path := filepath.Join(dir, KeyFileName(keyID))
	writeKey(t, path)

	signer, err := BuildSigner(SignerConfig{Type: "dir", Path: dir})
	require.NoError(t, err)

	before, err := signer.Sign(context.Background(), keyID, []byte("msg"))
	require.NoError(t, err)

	rotated := writeKey(t, path)
	// mtime の解像度に依存しないよう、明示的に進める。
	future := time.Now().Add(2 * time.Second)
	require.NoError(t, os.Chtimes(path, future, future))

	after, err := signer.Sign(context.Background(), keyID, []byte("msg"))
	require.NoError(t, err)
	require.NotEqual(t, before.Bytes, after.Bytes, "a rotated key must be picked up")

	local, err := httpsig.NewLocalSigner(keyID, rotated)
	require.NoError(t, err)
	want, err := local.Sign(context.Background(), keyID, []byte("msg"))
	require.NoError(t, err)
	require.Equal(t, want.Bytes, after.Bytes)
}

func TestDirSigner_RemovedKeyBecomesUnknown(t *testing.T) {
	dir := t.TempDir()
	keyID := "https://local.example/users/alice#main-key"
	path := filepath.Join(dir, KeyFileName(keyID))
	writeKey(t, path)

	signer, err := BuildSigner(SignerConfig{Type: "dir", Path: dir})
	require.NoError(t, err)
	_, err = signer.Sign(context.Background(), keyID, []byte("msg"))
	require.NoError(t, err)

	// 鍵を消したアクターの配送を続けない。
	require.NoError(t, os.Remove(path))
	_, err = signer.Sign(context.Background(), keyID, []byte("msg"))
	require.ErrorIs(t, err, httpsig.ErrUnknownKey)
}

func TestDirSigner_ConfigErrors(t *testing.T) {
	_, err := BuildSigner(SignerConfig{Type: "dir"})
	require.ErrorContains(t, err, "path is required")

	file := filepath.Join(t.TempDir(), "a-file")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))
	_, err = BuildSigner(SignerConfig{Type: "dir", Path: file})
	require.ErrorContains(t, err, "is not a directory")

	_, err = BuildSigner(SignerConfig{Type: "dir", Path: filepath.Join(t.TempDir(), "missing")})
	require.ErrorContains(t, err, "stat")
}

func TestBuildSigner_TypeErrors(t *testing.T) {
	_, err := BuildSigner(SignerConfig{})
	require.ErrorContains(t, err, "signer.type is required")

	_, err = BuildSigner(SignerConfig{Type: "http"})
	require.ErrorContains(t, err, "unknown signer type")
}

// The config path must reach the same executor the Go path builds.
func TestNewExecutor_FromConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "actor.pem")
	writeKey(t, path)

	cfg := executorConfig(t, "type: activitypub_deliver\n"+
		"timeout: 12s\n"+
		"allow_private_network: true\n"+
		"signer:\n"+
		"  type: file\n"+
		"  key_id: \""+testKeyID+"\"\n"+
		"  private_key_path: \""+path+"\"\n")

	ex, err := newExecutor(context.Background(), mkqd.BuildContext{Queue: "deliver"}, cfg)
	require.NoError(t, err)

	built, ok := ex.(*Executor)
	require.True(t, ok)
	require.Equal(t, 12*time.Second, built.timeout)
}

func TestNewExecutor_ConfigErrors(t *testing.T) {
	ctx := context.Background()
	bc := mkqd.BuildContext{Queue: "deliver"}

	_, err := newExecutor(ctx, bc, executorConfig(t, "type: activitypub_deliver"))
	require.ErrorContains(t, err, "signer.type is required")

	// 綴り間違いを黙って無視しない。
	_, err = newExecutor(ctx, bc, executorConfig(t, "type: activitypub_deliver\ntimeoout: 5s"))
	require.ErrorContains(t, err, "timeoout")
}

// executorConfig builds an ExecutorConfig through the real parser, the
// way the runtime does.
func executorConfig(t *testing.T, options string) mkqd.ExecutorConfig {
	t.Helper()
	var b strings.Builder
	b.WriteString("redis:\n  addrs: [\"127.0.0.1:6379\"]\nqueues:\n  - name: deliver\n    executor:\n")
	for _, line := range strings.Split(strings.TrimRight(options, "\n"), "\n") {
		b.WriteString("      " + line + "\n")
	}
	cfg, err := mkqd.ParseConfig([]byte(b.String()))
	require.NoError(t, err)
	require.NotNil(t, cfg.Queues[0].Executor)
	return *cfg.Queues[0].Executor
}
