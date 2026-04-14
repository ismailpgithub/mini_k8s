import {
  jobDispatchScheduler,
  jobCriScheduler,
  jobWatcherScheduler,
} from "./queues/queues.js";
import {
  jobDispatchWorker,
  jobCriWorker,
  jobWatchWorker,
} from "./queues/workers.js";

async function init() {
  await Promise.all([
    jobDispatchScheduler.upsertJobScheduler("job-dispatch-scheduler", {
      every: 2 * 1000,
    }),
    jobCriScheduler.upsertJobScheduler("job-cri-scheduler", {
      every: 5 * 1000,
    }),
    jobWatcherScheduler.upsertJobScheduler("job-watch-scheduler", {
      every: 10 * 1000,
    }),
  ]);
}

init();
