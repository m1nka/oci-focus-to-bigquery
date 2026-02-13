import { spawn } from "bun";
import { Config } from "./config";
import { info, debug, error } from "./logger";

export interface SyncResult {
  files_transferred: number;
  bytes_transferred: number;
  duration_seconds: number;
}

interface RcloneStats {
  bytes: number;
  checks: number;
  deletedDirs: number;
  deletes: number;
  elapsedTime: number;
  errors: number;
  fatalError: boolean;
  renames: number;
  retryError: boolean;
  serverSideCopies: number;
  serverSideCopyBytes: number;
  serverSideMoveBytes: number;
  serverSideMoves: number;
  speed: number;
  totalBytes: number;
  totalChecks: number;
  totalTransfers: number;
  transferTime: number;
  transfers: number;
}

function parseRcloneOutput(output: string): Partial<RcloneStats> {
  const stats: Partial<RcloneStats> = {};

  // Try to parse JSON stats line if present
  const lines = output.split("\n");
  for (const line of lines) {
    if (line.includes('"transfers"') && line.includes('"bytes"')) {
      try {
        const jsonStats = JSON.parse(line);
        return jsonStats;
      } catch {
        // Continue with regex parsing
      }
    }
  }

  // Fallback: Parse text output
  const transferredMatch = output.match(
    /Transferred:\s+(\d+)\s*\/\s*(\d+),\s*(\d+)%/
  );
  if (transferredMatch) {
    stats.transfers = parseInt(transferredMatch[1], 10);
    stats.totalTransfers = parseInt(transferredMatch[2], 10);
  }

  const bytesMatch = output.match(
    /Transferred:\s+([\d.]+)\s*([KMGT]?i?B)/i
  );
  if (bytesMatch) {
    let bytes = parseFloat(bytesMatch[1]);
    const unit = bytesMatch[2].toUpperCase();
    if (unit.includes("K")) bytes *= 1024;
    else if (unit.includes("M")) bytes *= 1024 * 1024;
    else if (unit.includes("G")) bytes *= 1024 * 1024 * 1024;
    else if (unit.includes("T")) bytes *= 1024 * 1024 * 1024 * 1024;
    stats.bytes = Math.round(bytes);
  }

  return stats;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024)
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

export async function syncOciToGcs(config: Config): Promise<SyncResult> {
  const startTime = Date.now();

  const source = `${config.oci.remoteName}:${config.oci.tenancyOcid}/FOCUS Reports`;
  const destination = `${config.gcs.remoteName}:${config.gcs.stagingBucket}/FOCUS-Reports`;

  info("SYNC", `Starting rclone sync from OCI to GCS staging bucket...`);
  debug("SYNC", `Source: ${source}`);
  debug("SYNC", `Destination: ${destination}`);

  if (config.job.dryRun) {
    info("SYNC", "DRY RUN mode - listing files that would be synced...");

    const args = [
      "lsf",
      source,
      "--recursive",
      "--config",
      "rclone.conf",
    ];

    debug("SYNC", `Running: rclone ${args.join(" ")}`);

    const proc = spawn({
      cmd: ["rclone", ...args],
      stdout: "pipe",
      stderr: "pipe",
    });

    const stdout = await new Response(proc.stdout).text();
    const stderr = await new Response(proc.stderr).text();
    const exitCode = await proc.exited;

    if (exitCode !== 0) {
      error("SYNC", `rclone lsf failed: ${stderr}`);
      throw new Error(`rclone lsf failed with exit code ${exitCode}`);
    }

    const files = stdout.trim().split("\n").filter(Boolean);
    info("SYNC", `DRY RUN: Would sync ${files.length} files`);

    if (files.length <= 20) {
      files.forEach((f) => debug("SYNC", `  - ${f}`));
    } else {
      files.slice(0, 10).forEach((f) => debug("SYNC", `  - ${f}`));
      debug("SYNC", `  ... and ${files.length - 10} more files`);
    }

    return {
      files_transferred: 0,
      bytes_transferred: 0,
      duration_seconds: Math.round((Date.now() - startTime) / 1000),
    };
  }

  // Build rclone sync command
  const args = [
    "sync",
    source,
    destination,
    "--config",
    "rclone.conf",
    "--stats",
    "1s",
    "--stats-one-line",
    "--stats-log-level",
    "NOTICE",
    "-v",
  ];

  debug("SYNC", `Running: rclone ${args.join(" ")}`);

  const proc = spawn({
    cmd: ["rclone", ...args],
    stdout: "pipe",
    stderr: "pipe",
  });

  const stdout = await new Response(proc.stdout).text();
  const stderr = await new Response(proc.stderr).text();
  const exitCode = await proc.exited;

  const combinedOutput = stdout + "\n" + stderr;

  if (exitCode !== 0) {
    error("SYNC", `rclone sync failed with exit code ${exitCode}`);
    error("SYNC", stderr);
    throw new Error(`rclone sync failed with exit code ${exitCode}`);
  }

  const stats = parseRcloneOutput(combinedOutput);
  const filesTransferred = stats.transfers || 0;
  const bytesTransferred = stats.bytes || 0;
  const durationSeconds = Math.round((Date.now() - startTime) / 1000);

  info(
    "SYNC",
    `Transferred ${filesTransferred.toLocaleString()} files (${formatBytes(bytesTransferred)})`
  );
  info("SYNC", `Sync completed in ${durationSeconds} seconds`);

  return {
    files_transferred: filesTransferred,
    bytes_transferred: bytesTransferred,
    duration_seconds: durationSeconds,
  };
}
