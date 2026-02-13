import { loadConfig, Config } from "./config";
import { setLogLevel, info, error } from "./logger";
import { syncOciToGcs, SyncResult } from "./sync";
import { reorganizeToHive, ReorganizeResult } from "./reorganize";

interface JobStatistics {
  sync: SyncResult;
  reorganize: ReorganizeResult;
  total_duration_seconds: number;
}

async function main(): Promise<void> {
  const totalStartTime = Date.now();
  let config: Config;

  try {
    // Load and validate configuration
    info("MAIN", "Loading configuration...");
    config = loadConfig();
    setLogLevel(config.job.logLevel);

    info("MAIN", "Configuration loaded successfully");
    info("MAIN", `Mode: ${config.job.syncMode}`);
    info("MAIN", `Dry run: ${config.job.dryRun}`);

    if (config.job.syncMode === "incremental") {
      info("MAIN", `Days to sync: ${config.job.daysToSync}`);
    }
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    error("MAIN", `Configuration error: ${errorMessage}`);
    process.exit(1);
  }

  let syncResult: SyncResult;
  let reorganizeResult: ReorganizeResult;

  try {
    // Step 1: Sync OCI to GCS staging
    info("MAIN", "=== Step 1: OCI to GCS Sync ===");
    syncResult = await syncOciToGcs(config);
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    error("MAIN", `Sync step failed: ${errorMessage}`);
    process.exit(1);
  }

  try {
    // Step 2: Reorganize to Hive partitioning
    info("MAIN", "=== Step 2: Hive Partitioning ===");
    reorganizeResult = await reorganizeToHive(config);
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    error("MAIN", `Reorganize step failed: ${errorMessage}`);
    process.exit(1);
  }

  const totalDurationSeconds = Math.round((Date.now() - totalStartTime) / 1000);

  // Output final statistics
  const statistics: JobStatistics = {
    sync: syncResult,
    reorganize: reorganizeResult,
    total_duration_seconds: totalDurationSeconds,
  };

  info("MAIN", "=== Job Statistics ===");
  console.log(JSON.stringify(statistics, null, 2));

  // Exit with error if any files had errors during reorganization
  if (reorganizeResult.files_errored > 0) {
    error(
      "MAIN",
      `Job completed with ${reorganizeResult.files_errored} errors`
    );
    process.exit(1);
  }

  info("MAIN", "Job completed successfully");
  process.exit(0);
}

main().catch((err) => {
  error("MAIN", `Unexpected error: ${err}`);
  process.exit(1);
});
