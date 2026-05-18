# Pulp-ext-docker

Docker/Podman spawn capability for Pulp cells, backed by [Potassium](https://github.com/BananaLabs-OSS/Potassium)'s Docker provider. Gives cells the ability to create, destroy, restart, exec, read logs, poll events, and read/write files inside containers.

From [BananaLabs OSS](https://github.com/BananaLabs-OSS).

## Deployment

```go
import _ "github.com/BananaLabs-OSS/Pulp-ext-docker"
```

## Capability

- `spawn.docker` — full container lifecycle + file ops
- Bananagine is the primary consumer; Evolution uses the file-ops subset

## Environment

The Docker socket must be accessible to the Pulp host process (`DOCKER_HOST` env var or the default Unix socket).
