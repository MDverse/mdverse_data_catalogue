FROM debian:13.1-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV APPLICATION_PATH=/opt

# Only use the managed Python version
ENV UV_PYTHON_PREFERENCE=only-managed

# Allow bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# The uv installer requires curl and certificates to download the release archive
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists /var/cache/apt/archives

# Create a non-root user
RUN groupadd --system --gid 999 nonrootuser && \
    useradd --system --gid 999 --uid 999 --create-home nonrootuser

WORKDIR ${APPLICATION_PATH}

RUN chown nonrootuser:nonrootuser ${APPLICATION_PATH}

# Download uv installer
ADD --chown=nonrootuser:nonrootuser https://astral.sh/uv/0.11.8/install.sh /tmp/uv-installer.sh

# Use the non-root user to install uv, install and run the application
USER nonrootuser

RUN sh /tmp/uv-installer.sh && rm /tmp/uv-installer.sh

# Ensure the installed binary is in the `PATH`
ENV PATH="/home/nonrootuser/.local/bin/:$PATH"

# Copy necessary files
COPY --chown=nonrootuser:nonrootuser ./pyproject.toml ${APPLICATION_PATH}/
# README.md is listed in pyproject.toml and is required to install the mdverse package.
COPY --chown=nonrootuser:nonrootuser ./README.md ${APPLICATION_PATH}/
COPY --chown=nonrootuser:nonrootuser ./uv.lock ${APPLICATION_PATH}/
COPY --chown=nonrootuser:nonrootuser ./src ${APPLICATION_PATH}/src
COPY --chown=nonrootuser:nonrootuser ./webapp ${APPLICATION_PATH}/webapp
COPY --chown=nonrootuser:nonrootuser ./data/database.db ${APPLICATION_PATH}/data/database.db

# Create a virtual environment and install dependencies
RUN uv sync --locked --no-dev

# Run the FastAPI application
# by defaut, FastAPI runs on port 8000
CMD ["uv", "run", "uvicorn", "webapp.app.main:app", "--host", "0.0.0.0", "--proxy-headers"]
