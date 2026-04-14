import { Worker } from "bullmq";
import { db } from "../db/index.js";
import { jobsTable, jobStatusEnumValues } from "../db/schema.js";
import { inArray, sql, eq } from "drizzle-orm";
import Docker from "dockerode";

const docker = new Docker({ socketPath: "//./pipe/docker_engine" });

function pullImage(image) {
  return new Promise(async (res) => {
    const stream = await docker.pull(image);
    docker.modem.followProgress(stream, () => {
      res();
    });
  });
}

export const jobDispatchWorker = new Worker(
  "job-dispatcher",
  async () => {
    console.log("[JobDispatcher] : Checking For New Submitted Jobs...");
    await db.transaction(
      async (tx) => {
        const stmt = sql`
            SELECT 
                id 
            FROM ${jobsTable} 
            WHERE ${jobsTable.state} = ${jobStatusEnumValues[0]} 
            ORDER BY ${jobsTable.createdAt} ASC
            FOR UPDATE SKIP LOCKED 
            LIMIT 5;
        `;

        const result = await tx.execute(stmt);
        const jobIds = result.rows.map((e) => e.id);

        console.log(
          `[JobDispatcher] : Found ${jobIds.length} jobs in submitted state`,
          jobIds,
        );

        //TODO: Check if compute is available
        if (jobIds.length > 0) {
          console.log(
            `[jobDispatcher]: Moving ${jobIds.length} jobs to Runnable State`,
          );

          await tx
            .update(jobsTable)
            .set({ state: "RUNNABLE" })
            .where(inArray(jobsTable.id, jobIds));
        }
      },
      { accessMode: "read write", isolationLevel: "read committed" },
    );
  },
  {
    connection: {
      host: "127.0.0.1",
      port: 6379,
    },
  },
);

export const jobCriWorker = new Worker(
  "job-cri",
  async () => {
    console.log("[JobDispatcher] : Checking For Runnable Jobs...");
    await db.transaction(
      async (tx) => {
        const stmt = sql`
            SELECT 
                id 
            FROM ${jobsTable} 
            WHERE ${jobsTable.state} = ${jobStatusEnumValues[1]} 
            ORDER BY ${jobsTable.createdAt} ASC
            FOR UPDATE SKIP LOCKED 
            LIMIT 1;
        `;

        const result = await tx.execute(stmt);
        const jobIds = result.rows.map((e) => e.id);

        console.log(
          `[JobDispatcher] : Found ${jobIds.length} jobs in Runnable state`,
          jobIds,
        );

        for (const jobId of jobIds) {
          const [job] = await db
            .select()
            .from(jobsTable)
            .where(eq(jobsTable.id, jobId));

          const checkImageResult = await docker.listImages({
            filters: {
              reference: [`${job.image}:latest`],
            },
          });

          // const filters = JSON.stringify({
          //   reference: [`${job.image}:latest`]
          // });

          // const checkImageResult = await docker.listImages({ filters });

          if (!checkImageResult || checkImageResult.length <= 0) {
            console.log(`Pulling Image ${job.image}:latest`);
            await pullImage(`${job.image}:latest`);
          }

          const container = docker.createContainer({
            Image: `${job.image}:latest`,
            Tty: false,
            HostConfig: {
              AutoRemove: false,
            },
            Cmd: job.cmd,
          });
          container.then(async (c) => {
            await c.start();
            console.log(`Container is Up and Running`);
            await tx
              .update(jobsTable)
              .set({ state: "RUNNING", containerId: c.id })
              .where(eq(jobsTable.id, job.id));
          });
        }
      },
      { accessMode: "read write", isolationLevel: "read committed" },
    );
  },
  {
    connection: {
      host: "127.0.0.1",
      port: 6379,
    },
  },
);

export const jobWatchWorker = new Worker(
  "job-watch",
  async () => {
    await db.transaction(
      async (tx) => {
        const stmt = sql`
            SELECT 
                id 
            FROM ${jobsTable} 
            WHERE ${jobsTable.state} = ${jobStatusEnumValues[2]} 
            ORDER BY ${jobsTable.createdAt} ASC
            FOR UPDATE
            LIMIT 5;
        `;

        const result = await tx.execute(stmt);
        const jobIds = result.rows.map((e) => e.id);

        for (const jobId of jobIds) {
          const [job] = await db
            .select()
            .from(jobsTable)
            .where(eq(jobsTable.id, jobId));

          if (job.containerId) {
            const container = await docker.getContainer(job.containerId);
            const containerStatus = await container.inspect();
            console.log(
              "hey I am inside the if condition",
              containerStatus.State.Status,
            );
            if (containerStatus.State.Status === "exited") {
              await tx
                .update(jobsTable)
                .set({ state: "SUCCEEDED", containerId: null })
                .where(eq(jobsTable.id, jobId));
              await container.remove();
            }
          }
        }
      },
      { accessMode: "read write", isolationLevel: "read committed" },
    );
  },
  {
    connection: {
      host: "127.0.0.1",
      port: 6379,
    },
  },
);
