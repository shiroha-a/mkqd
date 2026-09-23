# mkqd container image.
#
# Two stages: a toolchain image to build a static binary, and a
# distroless base that carries nothing else. The result runs as a
# non-root user and holds one file.

# **go.mod と同じパッチ版まで固定する。** 浮動タグ (golang:1.27) だと
# ビルドのたびに中身が変わって再現性が無く、govulncheck が見るのは go.mod 側
# なので、ここがずれていると **CI は緑のまま配る image だけが古い** という
# 状態が起きうる。CI がこの一致を検査している。
FROM golang:1.27.1 AS build

WORKDIR /src

# 依存だけ先に取る。ソースを触っただけのビルドで毎回取り直さない。
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# VERSION はリリース時に渡す。渡さなければ version.go の値が残り、
# 開発版であることがそのまま `mkqd version` に出る。
ARG VERSION=""
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags "-s -w${VERSION:+ -X github.com/shiroha-a/mkqd.Version=$VERSION}" \
    -o /out/mkqd ./cmd/mkqd

# static: libc も shell も無い。mkqd は CGO 無しの 1 バイナリなので足りる。
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/mkqd /usr/local/bin/mkqd

# 設定はマウントして渡す。`mkqd` のどのサブコマンドも MKQD_CONFIG を読む
# ので、compose や k8s 側で上書きする必要はない。
ENV MKQD_CONFIG=/etc/mkqd/mkqd.yaml

# distroless の nonroot は uid 65532。イメージ側で固定しておくと、
# runAsNonRoot だけ指定した Pod でも uid の指定漏れで落ちない。
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/mkqd"]
CMD ["run"]
