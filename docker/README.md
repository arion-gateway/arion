# Arion Docker Setup

This document provides detailed instructions for building and running Arion using Docker.

## Building the Image

```bash
# Initialize git submodules before building
git submodule update --init --recursive

docker build -t arion-proxy -f docker/Dockerfile .
```

## Running the Container

### Basic Setup

Use port mapping:

```bash
docker run -d \
  -p 8000:8000 \
  --name arion-proxy \
  arion-proxy
```

For full functionality (including TCP proxy):

```bash
docker run -d \
  -p 8000:8000 \
  -p 8001:8001 \
  --name arion-proxy \
  arion-proxy
```

Alternative with host networking:

```bash
docker run -d \
  --network host \
  --name arion-proxy \
  arion-proxy
```

> **Note**: Host networking may not work in all Docker environments (e.g., Docker Desktop on macOS/Windows). Use port mapping for reliable access to services.

## Configuration Options

### Custom Configuration File

Mount your own configuration file:

```bash
docker run -d \
  --network host \
  -v /path/to/your/config.yaml:/etc/arion/arion-runtime.yaml \
  --name arion-proxy \
  arion-proxy
```

### Control Plane Setup

Configure with external control plane:

```bash
docker run -d \
  --network host \
  -e CONTROL_PLANE_IP=<your_control_plane_ip> \
  --name arion-proxy \
  arion-proxy
```

### Environment Variables

| Variable           | Description                              | Default     |
| ------------------ | ---------------------------------------- | ----------- |
| `CONTROL_PLANE_IP` | IP address of the control plane          | `127.0.0.1` |
| `LOG_LEVEL`        | Logging level (debug, info, warn, error) | `info`      |
| `ARION_CPU_LIMIT`  | Number of CPU cores/threads to use. Set this to control Arion's concurrency, especially in containers. | (auto-detect) |
### Example: Setting CPU Limit in Docker

To explicitly set the number of CPU cores/threads Arion should use:

```bash
docker run -d \
  -e ARION_CPU_LIMIT=2 \
  --name arion-proxy \
  arion-proxy
```

Or, in Kubernetes, use the Downward API as shown in the main README.

## Verification

### Service Health Check
Once the container is running, verify the HTTP listener is responding.

**If using port mapping (`-p 8000:8000`):**
```bash
curl -v http://localhost:8000/direct-response # Should return HTTP 200 with "meow! 🐱"
```

**If using host networking (`--network host`) and it's working:**
```bash
curl -v http://localhost:8000/direct-response # Should return HTTP 200 with "meow! 🐱"
```

### Load Balancing Test
Test the default route that forwards to the configured backend cluster:

```bash
curl -v http://localhost:8000/ # Will attempt to proxy to backend (may timeout if backends aren't running)
```

### Setting up Backend Servers for Testing

The default configuration points to backend servers at `127.0.0.1:4001` and `127.0.0.1:4002`. To test with actual backends:

**Using Docker Network (to fix container networking):**

```bash
# Create a custom network
docker network create arion-net

# Start test backend servers on the network
docker run -d --network arion-net --name backend1 nginx:alpine
docker run -d --network arion-net --name backend2 nginx:alpine

# Update config to use container names instead of localhost
cp arion-proxy/conf/arion-runtime.yaml my-config.yaml
sed -i 's/127.0.0.1:4001/backend1:80/g' my-config.yaml
sed -i 's/127.0.0.1:4002/backend2:80/g' my-config.yaml

# Run Arion on the same network
docker run -d \
  --network arion-net \
  -p 8000:8000 \
  -v $(pwd)/my-config.yaml:/etc/arion/arion-runtime.yaml \
  --name arion-proxy \
  arion-proxy
```

**Alternative with host networking:**

```bash
# Start backends with port mapping
docker run -d -p 4001:80 --name backend1 nginx:alpine
docker run -d -p 4002:80 --name backend2 nginx:alpine

# Run Arion with host networking (uses default config)
docker run -d \
  --network host \
  --name arion-proxy \
  arion-proxy
```

For production deployment, update the cluster configuration in `arion-runtime.yaml` to point to your actual backend service IPs and ports.

## Port Reference

| Port  | Purpose                | Required for Basic Setup  |
| ----- | ---------------------- | ------------------------- |
| 8000  | HTTP Listener          | ✅ Yes (primary service)   |
| 8001  | TCP Listener           | ❌ Only if using TCP proxy |
| 50051 | xDS gRPC Configuration | ❌ Only with external xDS  |

> **Note**: Only port 8000 is required for basic HTTP proxy functionality. Map additional ports only when needed for specific features.
