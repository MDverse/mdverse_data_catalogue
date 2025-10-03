FROM debian:13.1-slim

# Only use the managed Python version
ENV UV_PYTHON_PREFERENCE=only-managed

# Allow bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# The installer requires curl (and certificates) to download the release archive
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates


# Setup a non-root user
RUN groupadd --system --gid 999 nonrootuser \
 && useradd --system --gid 999 --uid 999 --create-home nonrootuser

WORKDIR /opt

RUN chown nonrootuser:nonrootuser /opt

# Download uv installer (as root)
ADD https://astral.sh/uv/0.8.22/install.sh /opt/uv-installer.sh

# Run the installer as nonrootuser, then remove it
RUN chown nonrootuser:nonrootuser /opt/uv-installer.sh

# Use the non-root user to run our application
USER nonrootuser

RUN sh /opt/uv-installer.sh && rm /opt/uv-installer.sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/home/nonrootuser/.local/bin/:$PATH"


COPY ./pyproject.toml .
COPY ./uv.lock .

COPY ./app ./app
COPY ./templates ./templates
COPY ./static ./static
COPY ./database.db .

RUN uv sync --locked --no-dev 

# Place executables in the environment at the front of the path
#ENV PATH="/opt/.venv/bin:$PATH"

# Run the FastAPI application by default
# Uses `--host 0.0.0.0` to allow access from outside the container
CMD ["uv", "run", "fastapi", "dev", "app/main.py", "--host", "0.0.0.0", "--proxy-headers"]
