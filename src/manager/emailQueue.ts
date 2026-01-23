import { Queue } from "../queue.js";
import { Worker } from "../worker.js";

const emailQueue = new Queue("emailQueue");

await emailQueue.add({
  to: "user2@gmail.com",
  subject: "Welcome"
});

await emailQueue.add({
  to: "user3@gmail.com",
  subject: "Welcome"
});

const worker = new Worker(
  "emailQueue",
  async (job) => {
    console.log("Sending email to", job.data.to);
  }
);

await worker.start();
