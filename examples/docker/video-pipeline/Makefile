# Make targets for the video-pipeline demo. Short-hand over `docker compose`
# for the common tasks. The coordinator submits a fresh demo workflow every
# 5s on its own, so there's no `make submit` — just `make up` and watch.

.PHONY: up down logs rebuild scale-gpu scale-cpu

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f coordinator worker-gpu worker-cpu worker-def

rebuild:
	docker compose build --no-cache

# Scale a worker pool: `make scale-gpu N=3`
scale-gpu:
	docker compose up -d --scale worker-gpu=$(N)

scale-cpu:
	docker compose up -d --scale worker-cpu=$(N)
