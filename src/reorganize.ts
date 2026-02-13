import { Storage } from "@google-cloud/storage";
import { Config } from "./config";
import { info, debug, error } from "./logger";

export interface ReorganizeResult {
  files_processed: number;
  files_copied: number;
  files_skipped: number;
  files_errored: number;
  duration_seconds: number;
}

interface ParsedFile {
  year: string;
  month: string;
  day: string;
  filename: string;
  fullPath: string;
  date: Date;
}

const FILE_PATH_REGEX =
  /^FOCUS-Reports\/(\d{4})\/(\d{2})\/(\d{2})\/(.+\.csv\.gz)$/;

function parseFilePath(path: string): ParsedFile | null {
  const match = path.match(FILE_PATH_REGEX);
  if (!match) return null;

  const [, year, month, day, filename] = match;
  return {
    year,
    month,
    day,
    filename,
    fullPath: path,
    date: new Date(parseInt(year), parseInt(month) - 1, parseInt(day)),
  };
}

function isWithinDateRange(fileDate: Date, daysToSync: number): boolean {
  const now = new Date();
  const cutoffDate = new Date(now);
  cutoffDate.setDate(cutoffDate.getDate() - daysToSync);
  cutoffDate.setHours(0, 0, 0, 0);

  return fileDate >= cutoffDate;
}

function buildHivePath(parsed: ParsedFile): string {
  return `year=${parsed.year}/month=${parsed.month}/day=${parsed.day}/${parsed.filename}`;
}

async function ensureBucketExists(
  storage: Storage,
  bucketName: string,
  location: string
): Promise<boolean> {
  const bucket = storage.bucket(bucketName);
  const [exists] = await bucket.exists();

  if (!exists) {
    info("REORGANIZE", `Creating bucket: ${bucketName}`);
    await storage.createBucket(bucketName, { location });
    info("REORGANIZE", `Bucket created: ${bucketName}`);
    return true;
  } else {
    debug("REORGANIZE", `Bucket exists: ${bucketName}`);
    return true;
  }
}

async function checkBucketExists(
  storage: Storage,
  bucketName: string
): Promise<boolean> {
  const bucket = storage.bucket(bucketName);
  const [exists] = await bucket.exists();
  return exists;
}

export async function reorganizeToHive(
  config: Config
): Promise<ReorganizeResult> {
  const startTime = Date.now();

  info("REORGANIZE", "Starting Hive partitioning reorganization...");
  debug("REORGANIZE", `Source bucket: ${config.gcs.stagingBucket}`);
  debug("REORGANIZE", `Destination bucket: ${config.gcs.hiveBucket}`);
  debug("REORGANIZE", `Sync mode: ${config.job.syncMode}`);

  if (config.job.syncMode === "incremental") {
    debug("REORGANIZE", `Days to sync: ${config.job.daysToSync}`);
  }

  const storage = new Storage({ projectId: config.gcs.projectId });
  const bucketLocation = "europe-west3";

  // In dry run mode, just check if buckets exist and report
  if (config.job.dryRun) {
    const stagingExists = await checkBucketExists(storage, config.gcs.stagingBucket);
    const hiveExists = await checkBucketExists(storage, config.gcs.hiveBucket);

    if (!stagingExists) {
      info("REORGANIZE", `DRY RUN: Would create bucket: ${config.gcs.stagingBucket}`);
      info("REORGANIZE", `DRY RUN: Staging bucket doesn't exist yet - cannot list files`);
      return {
        files_processed: 0,
        files_copied: 0,
        files_skipped: 0,
        files_errored: 0,
        duration_seconds: Math.round((Date.now() - startTime) / 1000),
      };
    }
    if (!hiveExists) {
      info("REORGANIZE", `DRY RUN: Would create bucket: ${config.gcs.hiveBucket}`);
    }
  } else {
    // Ensure buckets exist (create if needed)
    await ensureBucketExists(storage, config.gcs.stagingBucket, bucketLocation);
    await ensureBucketExists(storage, config.gcs.hiveBucket, bucketLocation);
  }

  const stagingBucket = storage.bucket(config.gcs.stagingBucket);
  const hiveBucket = storage.bucket(config.gcs.hiveBucket);

  // List all files in staging bucket
  info("REORGANIZE", "Listing files in staging bucket...");
  const [files] = await stagingBucket.getFiles({ prefix: "FOCUS-Reports/" });

  debug("REORGANIZE", `Found ${files.length} total files in staging bucket`);

  // Parse and filter files
  const parsedFiles: ParsedFile[] = [];
  let skippedNonMatching = 0;
  let skippedOutOfRange = 0;

  for (const file of files) {
    const parsed = parseFilePath(file.name);

    if (!parsed) {
      skippedNonMatching++;
      debug("REORGANIZE", `Skipping non-matching path: ${file.name}`);
      continue;
    }

    // Apply date filter for incremental mode
    if (config.job.syncMode === "incremental") {
      if (!isWithinDateRange(parsed.date, config.job.daysToSync)) {
        skippedOutOfRange++;
        debug("REORGANIZE", `Skipping out of range: ${file.name}`);
        continue;
      }
    }

    parsedFiles.push(parsed);
  }

  info(
    "REORGANIZE",
    `Processing ${parsedFiles.length} files (${skippedNonMatching} non-matching, ${skippedOutOfRange} out of date range)`
  );

  if (config.job.dryRun) {
    info("REORGANIZE", "DRY RUN mode - showing files that would be copied...");

    for (const parsed of parsedFiles) {
      const hivePath = buildHivePath(parsed);
      debug("REORGANIZE", `  ${parsed.fullPath} -> ${hivePath}`);
    }

    info("REORGANIZE", `DRY RUN: Would process ${parsedFiles.length} files`);

    return {
      files_processed: parsedFiles.length,
      files_copied: 0,
      files_skipped: 0,
      files_errored: 0,
      duration_seconds: Math.round((Date.now() - startTime) / 1000),
    };
  }

  // Process files
  let filesCopied = 0;
  let filesSkipped = 0;
  let filesErrored = 0;
  const errors: Array<{ path: string; error: string }> = [];

  for (let i = 0; i < parsedFiles.length; i++) {
    const parsed = parsedFiles[i];
    const hivePath = buildHivePath(parsed);

    try {
      // Check if destination exists (idempotency)
      const destFile = hiveBucket.file(hivePath);
      const [exists] = await destFile.exists();

      if (exists) {
        debug("REORGANIZE", `Already present: ${hivePath}`);
        filesSkipped++;
      } else {
        // Copy file to hive bucket
        const sourceFile = stagingBucket.file(parsed.fullPath);
        await sourceFile.copy(destFile);
        debug("REORGANIZE", `Copied: ${parsed.fullPath} -> ${hivePath}`);
        filesCopied++;
      }

      // Log progress every 100 files
      if ((i + 1) % 100 === 0) {
        info(
          "REORGANIZE",
          `Progress: ${i + 1}/${parsedFiles.length} files processed`
        );
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : String(err);
      error(
        "REORGANIZE",
        `Error copying ${parsed.fullPath} to ${hivePath}: ${errorMessage}`
      );
      errors.push({ path: parsed.fullPath, error: errorMessage });
      filesErrored++;
    }
  }

  const durationSeconds = Math.round((Date.now() - startTime) / 1000);

  info(
    "REORGANIZE",
    `Completed: Copied: ${filesCopied}, Skipped: ${filesSkipped}, Errors: ${filesErrored}`
  );
  info("REORGANIZE", `Reorganization completed in ${durationSeconds} seconds`);

  if (errors.length > 0) {
    error("REORGANIZE", "Errors encountered:");
    for (const e of errors) {
      error("REORGANIZE", `  ${e.path}: ${e.error}`);
    }
  }

  return {
    files_processed: parsedFiles.length,
    files_copied: filesCopied,
    files_skipped: filesSkipped,
    files_errored: filesErrored,
    duration_seconds: durationSeconds,
  };
}
