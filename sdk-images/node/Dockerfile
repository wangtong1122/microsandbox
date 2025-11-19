# First stage: Build the portal binary with Node.js feature enabled
FROM rust:slim AS builder
#替换为中国的镜像源
env http_proxy "http://192.168.55.19:7890"
env https_proxy "http://192.168.55.19:7890"


# Install dependencies and build the portal binary
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /usr/src/microsandbox

WORKDIR /usr/src/microsandbox
COPY . .
RUN cargo build --release --bin portal --features nodejs

# Second stage: Create the Node.js image
# Using latest LTS version (currently 20) from Chainguard's minimalist images
FROM node:22-slim
env http_proxy "http://192.168.55.19:7890"
env https_proxy "http://192.168.55.19:7890"
#替换为中国的镜像源
#deb http://th.archive.ubuntu.com/ubuntu jammy main    #添加该行到文件
#RUN echo "deb http://th.archive.ubuntu.com/ubuntu jammy main" > /etc/apt/sources.list
#RUN source /etc/apt/sources.list
RUN apt-get update
RUN apt-get install libc6
# Set environment variables
ENV NODE_ENV=development \
    NPM_CONFIG_LOGLEVEL=info

# Install system dependencies, set up directories, and install global npm packages in one layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    ca-certificates \
    build-essential \
    python3 \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    # Set up working directory for the node user
    && mkdir -p /home/node/work \
    && chown -R node:node /home/node \
    && mkdir -p /etc/microsandbox/portal \
    && chown -R node:node /etc/microsandbox \
    # Install global npm packages
    && npm install -g npm@latest \
       typescript \
       ts-node \
       nodemon \
       eslint \
       prettier

# Copy the portal binary from the builder stage and set permissions
COPY --from=builder /usr/src/microsandbox/target/release/portal /usr/local/bin/
RUN chmod +x /usr/local/bin/portal

# Switch to the non-root node user
USER node
WORKDIR /home/node/work

# Set a command that starts portal and keeps the container running
CMD ["bash", "-c", "echo 'Node.js environment with microsandbox-portal ready' && RUST_LOG=debug portal & tail -f /dev/null"]
