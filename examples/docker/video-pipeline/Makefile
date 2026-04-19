# Make targets for the video-pipeline demo. Short-hand over `docker compose`
# for the common tasks.

.PHONY: up down logs submit rebuild scale-gpu scale-cpu

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f coordinator worker-gpu worker-cpu worker-def

# Submit a workflow. Pass a video id like: make submit VIDEO=abc-123
submit:
	docker compose run --rm submit $(VIDEO)

rebuild:
	docker compose build --no-cache

scale-gpu:
	docker compose up -d --scale worker-gpu=$(N)

scale-cpu:
	docker compose up -d --scale worker-cpu=$(N)
