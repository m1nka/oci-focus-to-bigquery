FROM oven/bun:1 AS base

# Install rclone
RUN apt-get update && apt-get install -y \
    rclone \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy package files
COPY package.json bun.lock* ./

# Install dependencies
RUN bun install --frozen-lockfile || bun install

# Copy source code and config
COPY src/ ./src/
COPY rclone.conf ./
COPY tsconfig.json ./

# Run the job
CMD ["bun", "run", "src/index.ts"]
