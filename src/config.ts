export interface Config {
  oci: {
    remoteName: string;
    tenancyOcid: string;
  };
  gcs: {
    remoteName: string;
    stagingBucket: string;
    hiveBucket: string;
    projectId: string;
  };
  job: {
    syncMode: "full" | "incremental";
    daysToSync: number;
    dryRun: boolean;
    logLevel: "debug" | "info";
  };
}

function getRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function getOptionalEnv(name: string, defaultValue: string): string {
  return process.env[name] || defaultValue;
}

export function loadConfig(): Config {
  return {
    oci: {
      remoteName: getOptionalEnv("OCI_RCLONE_REMOTE", "oci-usage-reports-config"),
      tenancyOcid: getRequiredEnv("OCI_TENANCY_OCID"),
    },
    gcs: {
      remoteName: getOptionalEnv("GCS_RCLONE_REMOTE", "gcs-config"),
      stagingBucket: getRequiredEnv("GCS_STAGING_BUCKET"),
      hiveBucket: getRequiredEnv("GCS_HIVE_BUCKET"),
      projectId: getRequiredEnv("GCS_PROJECT_ID"),
    },
    job: {
      syncMode: getOptionalEnv("SYNC_MODE", "incremental") as
        | "full"
        | "incremental",
      daysToSync: parseInt(getOptionalEnv("DAYS_TO_SYNC", "7"), 10),
      dryRun: getOptionalEnv("DRY_RUN", "false").toLowerCase() === "true",
      logLevel: getOptionalEnv("LOG_LEVEL", "info") as "debug" | "info",
    },
  };
}
