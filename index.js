const express = require("express");
const fs = require("fs");
const Bull = require("bull");
const Redis = require("redis");
const { RateLimiterRedis } = require("rate-limiter-flexible");
require("dotenv").config();

// Setting up Redis connection
const redisClient = Redis.createClient();
const rateLimiter = new RateLimiterRedis({
  storeClient: redisClient,
  keyPrefix: "rlflx",
  points: 20,
  duration: 60,
  execEvenly: false,
});

// Creating a task queue using Bull
const taskQueue = new Bull("taskQueue", {
  redis: {
    host: "127.0.0.1",
    port: 6379,
  },
});

const app = express();
const port = 3000;
const apiEndpoint = process.env.API_ENDPOINT;
const logFile = process.env.LOG_FILE;

app.use(express.json());

// Function to log the task
async function task(user_id) {
  const logMessage = `${user_id}-task completed at-${Date.now()}\n`;
  console.log(logMessage);

  // Store the log message in a log file
  fs.appendFile(logFile, logMessage, (err) => {
    if (err) {
      console.error("Failed to write to log file:", err);
    }
  });
}

// async function ensureRedisConnected(client) {
//   if (!client.isOpen) {
//     await client.connect();
//   }
// }

// Add tasks to the queue
app.post(apiEndpoint, async (req, res) => {
  const { user_id } = req.body;

  if (!user_id) {
    return res.status(400).json({ error: "user_id is required" });
  }

  try {
    await ensureRedisConnected(redisClient); // Ensure Redis client is connected
    await rateLimiter.consume(user_id, 1);

    await taskQueue.add({ user_id });
    res.status(200).json({ message: "Task queued" });
  } catch (err) {
    console.error("Error processing request:", err);

    if (err instanceof Error) {
      res.status(500).json({ error: "Internal Server Error" });
    } else {
      res.status(429).json({
        error: "Rate limit exceeded",
        retryAfter: Math.round(err.msBeforeNext / 1000),
      });
    }
  }
});

// Process tasks in the queue
taskQueue.process(async (job) => {
  const { user_id } = job.data;

  // Rate limit per second (1 task per second)
  await new Promise((resolve) => setTimeout(resolve, 1000));
  await task(user_id);
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
