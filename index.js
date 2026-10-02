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

const blobClient = new line.messagingApi.MessagingApiBlobClient({
  channelAccessToken: config.channelAccessToken,
});

const slipDir = process.env.SLIP_DIR || path.join(__dirname, "slips");
fs.mkdirSync(slipDir, { recursive: true });

// กัน event ซ้ำ (LINE redelivery) — production จริงควรใช้ Redis/DB
const seen = new Set();
function firstTime(id) {
  if (seen.has(id)) return false;
  seen.add(id);
  if (seen.size > 5000) seen.delete(seen.values().next().value);
  return true;
}

const app = express();

app.get("/health", (_req, res) => res.send("ok"));

app.post("/webhook", line.middleware(config), (req, res) => {
  res.sendStatus(200); // ตอบ LINE ทันที แล้วค่อยประมวลผล

  for (const event of req.body.events) {
    const id = event.webhookEventId || event.message?.id;
    if (id && !firstTime(id)) continue;

    handleEvent(event).catch((err) =>
      console.error("❌ handleEvent failed:", id, err)
    );
  }
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
    console.warn("Ignored image from non-allowed source");
    return;
  }

  const messageId = event.message.id;
  const stream = await blobClient.getMessageContent(messageId);

  const tmp = path.join(slipDir, `${messageId}.tmp`);
  const hash = crypto.createHash("sha256");
  stream.on("data", (chunk) => hash.update(chunk));

  try {
    await pipeline(stream, fs.createWriteStream(tmp));
  } catch (err) {
    await fs.promises.rm(tmp, { force: true });
    throw err;
  }

  const ext = await detectExt(tmp);
  const final = path.join(slipDir, `${Date.now()}-${messageId}.${ext}`);
  await fs.promises.rename(tmp, final);

  // TODO: อัปโหลดไป S3/R2 และบันทึก {sourceId, userId, sha256, timestamp} ลง DB
  console.log(`✅ Saved: ${final} sha256=${hash.digest("hex")}`);
}

const server = app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📁 Saving images to: ${slipDir}`);
});

process.on("SIGTERM", () => {
  console.log("SIGTERM received, shutting down");
  server.close(() => process.exit(0));
});
