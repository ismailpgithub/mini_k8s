import { Queue } from "bullmq";

export const jobDispatchScheduler = new Queue("job-dispatcher");
export const jobCriScheduler = new Queue("job-cri");
export const jobWatcherScheduler = new Queue("job-watch");
