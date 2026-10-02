require("dotenv").config();

const express = require("express");
const line = require("@line/bot-sdk");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { pipeline } = require("stream/promises");

for (const key of ["LINE_CHANNEL_SECRET", "LINE_CHANNEL_ACCESS_TOKEN"]) {
  if (!process.env[key]) {
    console.error(`Missing env: ${key}`);
    process.exit(1);
  }
}

const config = {
  channelSecret: process.env.LINE_CHANNEL_SECRET,
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN,
};

const PORT = process.env.PORT || 3000;

// ว่างไว้ = รับจากทุกคน; ใส่ userId/groupId คั่นด้วย comma เพื่อจำกัด
const ALLOWED = new Set(
  (process.env.ALLOWED_SOURCE_IDS || "").split(",").map((s) => s.trim()).filter(Boolean)
);
if (!ALLOWED.size) {
  console.warn("⚠️ ALLOWED_SOURCE_IDS ว่าง: กำลังรับรูปจากทุกคน");
}

const blobClient = new line.messagingApi.MessagingApiBlobClient({
  channelAccessToken: config.channelAccessToken,
});

// โฟลเดอร์เก็บสลิป: จำกัดสิทธิ์ให้เจ้าของเครื่องเท่านั้น
const slipDir = process.env.SLIP_DIR || path.join(__dirname, "slips");
fs.mkdirSync(slipDir, { recursive: true, mode: 0o700 });

// ลบไฟล์ .tmp ที่ค้างจากรอบก่อน (เช่น ปิดโปรแกรมกลางคัน)
for (const f of fs.readdirSync(slipDir)) {
  if (f.endsWith(".tmp")) fs.rmSync(path.join(slipDir, f), { force: true });
}

// กัน event ซ้ำ (LINE redelivery)
const seen = new Set();
function firstTime(id) {
  if (seen.has(id)) return false;
  seen.add(id);
  if (seen.size > 5000) seen.delete(seen.values().next().value);
  return true;
}

// งานที่กำลังประมวลผล เพื่อรอให้เสร็จก่อนปิดโปรแกรม
const inFlight = new Set();

async function withRetry(fn, tries = 3) {
  for (let i = 1; ; i++) {
    try {
      return await fn();
    } catch (err) {
      if (i >= tries) throw err;
      await new Promise((r) => setTimeout(r, 500 * 2 ** (i - 1)));
    }
  }
}

const app = express();

app.get("/health", (_req, res) => res.send("ok"));

app.post("/webhook", line.middleware(config), (req, res) => {
  res.sendStatus(200); // ตอบ LINE ทันที แล้วค่อยประมวลผล

  for (const event of req.body.events) {
    const id = event.webhookEventId || event.message?.id;
    if (id && !firstTime(id)) continue;

    const p = handleEvent(event)
      .catch((err) => {
        if (id) seen.delete(id);
        console.error("❌ handleEvent failed:", id, err);
      })
      .finally(() => inFlight.delete(p));
    inFlight.add(p);
  }
});

// signature ไม่ถูกต้อง = 401 (ไม่ใช่ 500)
app.use((err, _req, res, _next) => {
  if (err instanceof line.SignatureValidationFailed) {
    return res.status(401).send("invalid signature");
  }
  if (err instanceof line.JSONParseError) {
    return res.status(400).send("bad request");
  }
  console.error("Unhandled error:", err);
  res.status(500).send("error");
});

async function detectExt(filepath) {
  const fh = await fs.promises.open(filepath, "r");
  try {
    const buf = Buffer.alloc(8);
    await fh.read(buf, 0, 8, 0);
    if (buf[0] === 0xff && buf[1] === 0xd8) return "jpg";
    if (buf.toString("hex", 0, 4) === "89504e47") return "png";
    return "bin";
  } finally {
    await fh.close();
  }
}

async function handleEvent(event) {
  if (event.type !== "message" || event.message.type !== "image") return;

  const sourceId = event.source.groupId || event.source.roomId || event.source.userId;
  if (ALLOWED.size && !ALLOWED.has(sourceId)) {
    console.warn("Ignored image from non-allowed source:", sourceId);
    return;
  }

  const messageId = event.message.id;
  const tmp = path.join(slipDir, `${messageId}.tmp`);

  const digest = await withRetry(async () => {
    const stream = await blobClient.getMessageContent(messageId);
    const hash = crypto.createHash("sha256");
    stream.on("data", (chunk) => hash.update(chunk));
    try {
      await pipeline(stream, fs.createWriteStream(tmp, { mode: 0o600 }));
    } catch (err) {
      await fs.promises.rm(tmp, { force: true });
      throw err;
    }
    return hash.digest("hex");
  });

  const ext = await detectExt(tmp);
  const final = path.join(slipDir, `${Date.now()}-${messageId}.${ext}`);
  await fs.promises.rename(tmp, final);

  // เก็บข้อมูลว่าใครส่ง ส่งเมื่อไหร่ มาจากที่ไหน
  await fs.promises.writeFile(
    `${final}.json`,
    JSON.stringify(
      {
        messageId,
        sourceId,
        userId: event.source.userId || null,
        receivedAt: new Date(event.timestamp).toISOString(),
        sha256: digest,
        file: path.basename(final),
      },
      null,
      2
    ),
    { mode: 0o600 }
  );

  console.log(`✅ Saved: ${final} source=${sourceId} sha256=${digest}`);
}

const server = app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📁 Saving images to: ${slipDir}`);
});

async function shutdown(signal) {
  console.log(`${signal} received, waiting for ${inFlight.size} job(s)`);
  setTimeout(() => process.exit(1), 15000).unref(); // กันค้าง
  server.close();
  await Promise.allSettled([...inFlight]);
  process.exit(0);
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
