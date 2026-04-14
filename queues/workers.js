import { Worker } from "bullmq";
import { db } from "../db/index.js";
import { jobsTable, jobStatusEnumValues } from "../db/schema.js";
import { inArray, sql, eq } from "drizzle-orm";
import Docker from "dockerode";

const docker = new Docker({ socketPath: "//./pipe/docker_engine" });

// Helper for formatted logging
const log = (module, message, data = "") => {
  const timestamp = new Date().toLocaleTimeString();
  console.log(`[${timestamp}] [${module}] ${message}`, data);
};

async function ensureImage(imageName) {
  try {
    const filters = JSON.stringify({ reference: [imageName] });
    const images = await docker.listImages({ filters });

    if (images.length > 0) {
      log("CRI", `Image already exists: ${imageName}`);
      return;
    }

    log("CRI", `Pulling missing image: ${imageName}...`);
    return new Promise((resolve, reject) => {
      docker.pull(imageName, (err, stream) => {
        if (err) return reject(err);
        docker.modem.followProgress(stream, (err, res) =>
          err ? reject(err) : resolve(res),
        );
      });
    });
  } catch (err) {
    throw new Error(`Docker Pull Failed: ${err.message}`);
  }
}

// 1. JOB DISPATCHER
export const jobDispatchWorker = new Worker(
  "job-dispatcher",
  async () => {
    try {
      await db.transaction(async (tx) => {
        const result = await tx.execute(sql`
        SELECT id FROM ${jobsTable} 
        WHERE ${jobsTable.state} = ${jobStatusEnumValues[0]} 
        ORDER BY ${jobsTable.createdAt} ASC 
        FOR UPDATE SKIP LOCKED LIMIT 5
      `);

        const ids = result.rows.map((r) => r.id);
        if (ids.length === 0) return;

        log(
          "DISPATCHER",
          `Found ${ids.length} NEW jobs. Moving to RUNNABLE...`,
          ids,
        );

        await tx
          .update(jobsTable)
          .set({ state: "RUNNABLE" })
          .where(inArray(jobsTable.id, ids));
      });
    } catch (err) {
      log("DISPATCHER-ERROR", err.message);
    }
  },
  { connection: { host: "127.0.0.1", port: 6379 } },
);

// 2. CRI WORKER
export const jobCriWorker = new Worker(
  "job-cri",
  async () => {
    const job = await db.transaction(async (tx) => {
      const result = await tx.execute(sql`
      SELECT * FROM ${jobsTable} 
      WHERE ${jobsTable.state} = 'RUNNABLE' 
      ORDER BY ${jobsTable.createdAt} ASC 
      FOR UPDATE SKIP LOCKED LIMIT 1
    `);

      if (result.rows.length === 0) return null;
      const jobData = result.rows[0];

      await tx
        .update(jobsTable)
        .set({ state: "RUNNING" })
        .where(eq(jobsTable.id, jobData.id));
      return jobData;
    });

    if (!job) return;

    log("CRI", `Starting Job: ${job.id} | Image: ${job.image}`);

    try {
      const fullImage = job.image.includes(":")
        ? job.image
        : `${job.image}:latest`;

      await ensureImage(fullImage);

      log("CRI", `Creating container for ${job.id}...`);
      const container = await docker.createContainer({
        Image: fullImage,
        Cmd: job.cmd ? job.cmd.split(" ") : [],
        Labels: { "managed-by": "mini-k8s", "job-id": job.id },
      });

      await container.start();

      await db
        .update(jobsTable)
        .set({ containerId: container.id })
        .where(eq(jobsTable.id, job.id));

      log("CRI", `Container RUNNING: ${container.id.substring(0, 12)}`);
    } catch (err) {
      log("CRI-ERROR", `Job ${job.id} Failed: ${err.message}`);
      await db
        .update(jobsTable)
        .set({ state: "FAILED" })
        .where(eq(jobsTable.id, job.id));
    }
  },
  { connection: { host: "127.0.0.1", port: 6379 } },
);

// 3. WATCHER WORKER
export const jobWatchWorker = new Worker(
  "job-watch",
  async () => {
    try {
      const runningJobs = await db
        .select()
        .from(jobsTable)
        .where(eq(jobsTable.state, "RUNNING"));

      for (const job of runningJobs) {
        if (!job.containerId) {
          log(
            "WATCHER",
            `Job ${job.id} is in RUNNING state but has no containerId yet. Skipping...`,
          );
          continue;
        }

        try {
          const container = docker.getContainer(job.containerId);
          const inspect = await container.inspect();
          const currentStatus = inspect.State.Status;

          if (currentStatus === "exited") {
            const exitCode = inspect.State.ExitCode;
            const success = exitCode === 0;

            log(
              "WATCHER",
              `Container Exited. Job: ${job.id} | Status: ${success ? "SUCCESS" : "FAILED"} (Code: ${exitCode})`,
            );

            await db
              .update(jobsTable)
              .set({ state: success ? "SUCCEEDED" : "FAILED" })
              .where(eq(jobsTable.id, job.id));

            log(
              "WATCHER",
              `Cleaning up container ${job.containerId.substring(0, 12)}...`,
            );
            await container
              .remove()
              .catch((e) => log("WATCHER-CLEANUP-WARN", e.message));
          }
        } catch (err) {
          if (err.statusCode === 404) {
            log(
              "WATCHER-WARN",
              `Container ${job.containerId} not found in Docker. Marking job as FAILED.`,
            );
            await db
              .update(jobsTable)
              .set({ state: "FAILED" })
              .where(eq(jobsTable.id, job.id));
          } else {
            log(
              "WATCHER-ERROR",
              `Failed to inspect job ${job.id}: ${err.message}`,
            );
          }
        }
      }
    } catch (err) {
      log("WATCHER-GLOBAL-ERROR", err.message);
    }
  },
  { connection: { host: "127.0.0.1", port: 6379 } },
);
