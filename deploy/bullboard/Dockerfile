FROM node:22-alpine

WORKDIR /app

# **ioredis は自分で入れる必要がある。** BullMQ 6 で必須依存から optional
# peer dependency に変わったので、npm は自動では入れてくれない。入れずに
# `new Queue(...)` すると「could not load the optional 'ioredis' package」で
# 落ちる。bullmq を上げるときはここも見ること。
COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund

COPY server.js ./

USER node
EXPOSE 3000
CMD ["node", "server.js"]
