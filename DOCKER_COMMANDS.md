# Docker Commands

Run these from the repository root:

```powershell
cd E:\Documents\uni\master\water-quality-monitoring
```

## Normal Stop And Start

Use this when you want to stop the app and continue later without clearing Cassandra data.

```powershell
docker compose -f src/docker-compose.yml down
docker compose -f src/docker-compose.yml up -d
```

If you changed Dockerfiles, dependencies, or application code and want Docker to rebuild images:

```powershell
docker compose -f src/docker-compose.yml up -d --build
```

## Fresh Start From The Beginning

Use this when you want to remove all Docker data for this demo, including Cassandra data, Spark checkpoints, and notification data.

```powershell
docker compose -f src/docker-compose.yml down -v --remove-orphans
docker compose -f src/docker-compose.yml up -d --build --force-recreate
```

## Check That It Started

```powershell
docker compose -f src/docker-compose.yml ps
Invoke-WebRequest -UseBasicParsing http://localhost/healthcheck
Invoke-WebRequest -UseBasicParsing http://localhost:8001/healthcheck
```

## Useful Pages

- Sensor server control panel: http://localhost/control-panel
- Main dashboard: http://localhost:8001/digital-twin
- Kafka UI: http://localhost:8080
- Spark master UI: http://localhost:8081

## Useful Logs

```powershell
docker compose -f src/docker-compose.yml logs -f sensor-server
docker compose -f src/docker-compose.yml logs -f dashboard-backend
docker compose -f src/docker-compose.yml logs -f spark-master
docker compose -f src/docker-compose.yml logs -f notification-service
```

## What The Common Flags Mean

- `-d` starts containers in the background.
- `--build` rebuilds images before starting.
- `-v` removes named Docker volumes, including Cassandra data.
- `--remove-orphans` removes old containers that are no longer in the Compose file.
- `--force-recreate` recreates containers even if Docker thinks they are already up to date.
