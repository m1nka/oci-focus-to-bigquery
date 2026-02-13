export type LogLevel = "debug" | "info";
export type Step = "MAIN" | "SYNC" | "REORGANIZE";

let currentLogLevel: LogLevel = "info";

export function setLogLevel(level: LogLevel): void {
  currentLogLevel = level;
}

function formatTimestamp(): string {
  return new Date().toISOString();
}

function shouldLog(level: LogLevel): boolean {
  if (currentLogLevel === "debug") return true;
  return level !== "debug";
}

export function log(
  level: "DEBUG" | "INFO" | "ERROR",
  step: Step,
  message: string
): void {
  const logLevel = level.toLowerCase() as LogLevel;
  if (level !== "ERROR" && !shouldLog(logLevel)) return;

  const timestamp = formatTimestamp();
  console.log(`[${timestamp}] [${level}] [${step}] ${message}`);
}

export function debug(step: Step, message: string): void {
  log("DEBUG", step, message);
}

export function info(step: Step, message: string): void {
  log("INFO", step, message);
}

export function error(step: Step, message: string): void {
  log("ERROR", step, message);
}
