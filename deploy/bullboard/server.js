// bull-board over the queues mkqd writes.
//
// Nothing here is mkqd-aware: it is the stock BullMQ dashboard pointed
// at the same Redis keys. That is the point — if what mkqd writes
// stopped being BullMQ-shaped, this would stop rendering.

import express from "express";
import { Queue } from "bullmq";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { ExpressAdapter } from "@bull-board/express";

const host = process.env.REDIS_HOST ?? "127.0.0.1";
const port = Number(process.env.REDIS_PORT ?? 6379);
const prefix = process.env.BULL_PREFIX ?? "bull";
const names = (process.env.BULL_QUEUES ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

if (names.length === 0) {
  console.error("BULL_QUEUES is empty; set it to a comma-separated list of queue names");
  process.exit(2);
}

const queues = names.map(
  (name) => new Queue(name, { connection: { host, port }, prefix }),
);

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath("/");
createBullBoard({
  queues: queues.map((q) => new BullMQAdapter(q)),
  serverAdapter,
});

const app = express();
app.use("/", serverAdapter.getRouter());

const httpServer = app.listen(3000, () => {
  console.log(`bull-board on :3000 (prefix=${prefix}, queues=${names.join(",")})`);
});

// compose stop / k8s の SIGTERM で素直に落ちる。閉じ損ねると停止が
// grace period いっぱいまで待たされる。
for (const signal of ["SIGTERM", "SIGINT"]) {
  process.on(signal, async () => {
    httpServer.close();
    await Promise.all(queues.map((q) => q.close()));
    process.exit(0);
  });
}
