require("dotenv").config();

const express = require("express");
const line = require("@line/bot-sdk");
const fs = require("fs");
const path = require("path");

const config = {
  channelSecret: process.env.LINE_CHANNEL_SECRET,
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN,
};

const PORT = process.env.PORT || 3000;

const client = new line.messagingApi.MessagingApiClient({
  channelAccessToken: config.channelAccessToken,
});

const blobClient = new line.messagingApi.MessagingApiBlobClient({
  channelAccessToken: config.channelAccessToken,
});

const app = express();

const slipDir = path.join(__dirname, "slips");

if (!fs.existsSync(slipDir)) {
  fs.mkdirSync(slipDir, { recursive: true });
}

app.post(
  "/webhook",
  line.middleware(config),
  async (req, res) => {
    try {
      await Promise.all(
        req.body.events.map(handleEvent)
      );

      res.sendStatus(200);
    } catch (error) {
      console.error("❌ Webhook error:");
      console.error(error);

      res.sendStatus(500);
    }
  }
);

async function handleEvent(event) {
  console.log("Event:", event.type);

  if (
    event.type !== "message" ||
    event.message.type !== "image"
  ) {
    return;
  }

  console.log("📷 Image received");
  console.log("Message ID:", event.message.id);

  const stream = await blobClient.getMessageContent(
    event.message.id
  );

  const filename =
    `${Date.now()}-${event.message.id}.jpg`;

  const filepath = path.join(
    slipDir,
    filename
  );

  const writeStream = fs.createWriteStream(filepath);

  stream.pipe(writeStream);

  await new Promise((resolve, reject) => {
    writeStream.on("finish", resolve);
    writeStream.on("error", reject);
  });

  console.log(`✅ Saved: ${filepath}`);
}

app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`📁 Saving images to: ${slipDir}`);
});