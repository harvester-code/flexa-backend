PROJECT = flexa-waitfree-api

dev.up:
	@export $(shell cat .env | xargs) && \
	doppler run -- docker compose -f docker-compose.dev.yaml up -d
dev.down:
	docker compose -f docker-compose.dev.yaml down
