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
ADD --chown=nonrootuser:nonrootuser https://astral.sh/uv/0.8.22/install.sh /tmp/uv-installer.sh

# Use the non-root user to install uv, install and run the application
USER nonrootuser

RUN sh /tmp/uv-installer.sh && rm /tmp/uv-installer.sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/home/nonrootuser/.local/bin/:$PATH"

# Copy necessary files
COPY ./pyproject.toml .
COPY ./uv.lock .
COPY ./app ./app
COPY ./templates ./templates
COPY ./static ./static
COPY ./database.db .

# Create a virtual environment and install dependencies
RUN uv sync --locked --no-dev 

# Place executables in the environment at the front of the path
#ENV PATH="/opt/.venv/bin:$PATH"

# Run the FastAPI application
CMD ["uv", "run", "fastapi", "dev", "app/main.py", "--host", "0.0.0.0", "--proxy-headers"]
