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

## ABI surface (host imports)

| Function | Direction | Description |
|---|---|---|
| `docker_list` | req→resp | list containers (optional filter) |
| `docker_get` | req→resp | inspect a single container by name/ID |
| `docker_create` | req→resp | allocate + start a container |
| `docker_destroy` | req | stop + remove a container |
| `docker_restart` | req | restart a container |
| `docker_exec` | req→resp | run a command inside a container |
| `docker_logs` | req→resp | fetch tail of container stdout/stderr |
| `docker_stats` | req→resp | CPU/memory/net/disk stats for one container |
| `docker_stats_all` | req→resp | stats snapshot for all running containers |
| `docker_files_read` | req→resp | copy a file out of a container |
| `docker_files_write` | req | copy a file into a container |
| `docker_files_delete` | req | delete a file inside a container |
| `docker_events_poll` | req→resp | poll the event ring buffer since a cursor |
| `docker_build` | req | trigger an async `docker build` (non-blocking) |
| `docker_build_status` | req→resp | poll the current/last build status |

## Security

Bind-mount sources are validated against a deny-list of sensitive host paths and must fall under a configured allow-root (`DOCKER_BIND_ROOTS` env, colon-separated on Linux). With no roots configured all mounts are rejected (fail closed).

Container operations are scoped per cell: each cell can only target containers it created (name-prefixed `pulp-<cellID>-`). Set `DOCKER_SCOPE_DISABLE=1` to give a trusted orchestrator (Bananagine) unscoped access.

## Environment

The Docker socket must be accessible to the Pulp host process (`DOCKER_HOST` env var or the default Unix socket).
