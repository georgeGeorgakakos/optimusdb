# optimusCapacity client — for running the CP or RA side inside a container,
# or as a one-shot smoke test against a deployed agent.
#
#   docker build -t optimus-capacity .
#   docker run --rm -e OPTIMUSDB_URL=http://193.225.250.240/optimusdb1 \
#              optimus-capacity health
#
# Mount a CDT to submit one:
#   docker run --rm -v $(pwd)/samples:/cdt \
#              -e OPTIMUSDB_URL=... optimus-capacity \
#              submit-cdt <capID> --file /cdt/athens_gpu_pool.yaml

FROM python:3.11-slim

LABEL org.opencontainers.image.title="optimusCapacity" \
      org.opencontainers.image.description="Client for the OptimusDB Capacity Registry" \
      org.opencontainers.image.licenses="MIT"

WORKDIR /app

# Dependencies first so the layer caches across source edits.
COPY requirements.txt pyproject.toml ./
COPY capacity_client.py ./
RUN pip install --no-cache-dir -e ".[yaml]"

COPY samples/ ./samples/
COPY README.md EXAMPLE.md ./

# Non-root: this client only makes outbound HTTP calls.
RUN useradd --create-home --uid 10001 capacity && chown -R capacity:capacity /app
USER capacity

ENV OPTIMUSDB_URL=http://optimusdb1:8089 \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["optimus-capacity"]
CMD ["health"]
