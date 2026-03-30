FROM golang:1.19.13 AS builder

WORKDIR /optimusdbKB

# Install build dependencies INCLUDING unzip
RUN apt update && apt install -y \
    sqlite3 \
    libsqlite3-dev \
    libdqlite-dev \
    gcc \
    g++ \
    make \
    cmake \
    wget \
    curl \
    unzip

# Copy go.mod and go.sum first
COPY go.mod go.sum ./

# Clean go.mod from problematic dependencies (keep HTTP client working)
RUN sed -i '/go-llama/d' go.mod && \
    sed -i '/binding\/go-llama/d' go.mod

# Download dependencies
RUN go mod download || true

# Copy all source files INCLUDING your binding folder with libbinding.a
COPY . .

# Download pre-built llama.cpp server (instead of building from source)
RUN wget https://github.com/ggerganov/llama.cpp/releases/download/b3790/llama-b3790-bin-ubuntu-x64.zip && \
    unzip -j llama-b3790-bin-ubuntu-x64.zip "*/llama-server" -d /tmp/ && \
    chmod +x /tmp/llama-server && \
    rm llama-b3790-bin-ubuntu-x64.zip

# Download sqlite-vec loadable extension (.so) for semantic search.
# Loaded at runtime via SELECT load_extension() — no CGo bindings needed.
# Requires _allow_load_extension=1 in the DSN (set in app/app.go InitSQLite).
RUN mkdir -p /usr/lib/sqlite-vec && \
    wget -q https://github.com/asg017/sqlite-vec/releases/download/v0.1.6/sqlite-vec-0.1.6-loadable-linux-x86_64.tar.gz && \
    tar xzf sqlite-vec-0.1.6-loadable-linux-x86_64.tar.gz && \
    cp vec0.so /usr/lib/sqlite-vec/vec0.so && \
    rm sqlite-vec-0.1.6-loadable-linux-x86_64.tar.gz

# Remove problematic local binding imports but keep HTTP client
RUN echo "Configuring for HTTP-based TinyLlama..." && \
    find . -name "*.go" -type f -exec grep -l "binding/go-llama" {} \; | while read file; do \
        echo "Patching $file" && \
        sed -i '/binding\/go-llama/d' "$file" && \
        sed -i '/llama "optimusdb\/binding\/go-llama/d' "$file" && \
        sed -i '/gollamalib/d' "$file"; \
    done && \
    if [ -f "contextualmetadata/tinyllama_local.go" ]; then \
        echo "Removing tinyllama_local.go" && \
        rm -f contextualmetadata/tinyllama_local.go; \
    fi && \
    if [ -d "binding/go-llama.cpp" ]; then \
        echo "Removing go-llama.cpp directory" && \
        rm -rf binding/go-llama.cpp; \
    fi

# Tidy modules
RUN go mod tidy || true

# Build OptimusDB with HTTP client support
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o optimusdb main.go

# ==============================================================================
# Stage 2: Runtime
# ==============================================================================
FROM ubuntu:22.04

WORKDIR /root/

# Install runtime dependencies and supervisor for process management
RUN apt update && apt install -y \
    sqlite3 \
    libsqlite3-dev \
    libdqlite-dev \
    wget \
    curl \
    figlet \
    net-tools \
    iproute2 \
    htop \
    procps \
    vim \
    nano \
    fontconfig \
    fonts-dejavu-core \
    ca-certificates \
    supervisor \
    libgomp1 \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Copy binaries from builder stage
COPY --from=builder /tmp/llama-server /usr/local/bin/llama-server
COPY --from=builder /optimusdbKB/optimusdb /usr/local/bin/optimusdb

# Copy sqlite-vec loadable extension from builder stage
COPY --from=builder /usr/lib/sqlite-vec/vec0.so /usr/lib/sqlite-vec/vec0.so

# Create necessary directories
RUN mkdir -p /data/orbitdb /data/ipfs /config /models /var/log/supervisor /var/run

# Copy TinyLlama model from local project (models/ folder).
# The deploy script (deploy-optimusdb-scheduled.sh) copies the valid model
# from /opt/iccs/libs/ into running pods after kubectl apply, so a 0-byte
# placeholder here is acceptable — supervisord will restart tinyllama once
# the real file is in place.
COPY models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf /models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf

# Set executable permissions
RUN chmod +x /usr/local/bin/optimusdb /usr/local/bin/llama-server

# Create supervisor configuration file.
# Includes [unix_http_server] and [supervisorctl] so that
# 'supervisorctl restart tinyllama' works inside the container
# (required by the deploy script and manual recovery procedures).
RUN printf '[supervisord]\n\
nodaemon=true\n\
logfile=/var/log/supervisor/supervisord.log\n\
pidfile=/var/run/supervisord.pid\n\
childlogdir=/var/log/supervisor\n\
\n\
[unix_http_server]\n\
file=/var/run/supervisor.sock\n\
chmod=0700\n\
\n\
[supervisorctl]\n\
serverurl=unix:///var/run/supervisor.sock\n\
\n\
[rpcinterface:supervisor]\n\
supervisor.rpcinterface_factory=supervisor.rpcinterface:make_main_rpcinterface\n\
\n\
[program:tinyllama]\n\
command=/usr/local/bin/llama-server -m /models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf -c 2048 --host 127.0.0.1 --port 8080 --n-gpu-layers 0 --embedding\n\
autostart=true\n\
autorestart=true\n\
stdout_logfile=/var/log/supervisor/tinyllama.log\n\
stderr_logfile=/var/log/supervisor/tinyllama_info.log\n\
priority=1\n\
startretries=5\n\
startsecs=10\n\
\n\
[program:optimusdb]\n\
command=/usr/local/bin/optimusdb\n\
autostart=true\n\
autorestart=true\n\
stdout_logfile=/var/log/supervisor/optimusdb.log\n\
stderr_logfile=/var/log/supervisor/optimusdb_info.log\n\
priority=2\n\
startretries=3\n\
startsecs=20\n\
environment=TINYLLAMA_ENDPOINT="http://127.0.0.1:8080/v1/completions"\n\
\n\
[group:optimusdb-suite]\n\
programs=tinyllama,optimusdb\n' > /etc/supervisor/conf.d/supervisord.conf

# Environment variables
ENV TINYLLAMA_ENDPOINT="http://127.0.0.1:8080/v1/completions" \
    TINYLLAMA_MODEL="/models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf" \
    METADATA_ENRICHMENT_ENABLED="true" \
    METADATA_CACHE_TTL="24h"

# Expose ports
EXPOSE 4001 4002 5001 8080 8089 9001

# Health check for OptimusDB
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=5 \
    CMD pgrep -f optimusdb || exit 1

# Start supervisor directly
CMD ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisor/conf.d/supervisord.conf"]