PROJECT = flexa-waitfree-api

dev.up:
	@export $(shell cat .env | xargs) && \
	doppler run -- docker compose -f docker-compose.dev.yaml up -d
dev.down:
	docker compose -f docker-compose.dev.yaml down

.PHONY: build-develop
build-develop: ## Build the develop docker image.
	docker compose -f docker/develop/compose.yaml build

.PHONY: start-develop
start-develop: ## Start the develop docker container.
	docker compose -f docker/develop/compose.yaml up -d

.PHONY: stop-develop
stop-develop: ## Stop the develop docker container.
	docker compose -f docker/develop/compose.yaml down && docker rmi flexa-waitfree-api-develop

.PHONY: build-staging
build-staging: ## Build the staging docker image.
	docker compose -f docker/staging/compose.yaml build

.PHONY: start-staging
start-staging: ## Start the staging docker container.
	docker compose -f docker/staging/compose.yaml up -d

.PHONY: stop-staging
stop-staging: ## Stop the staging docker container.
	docker compose -f docker/staging/compose.yaml down && docker rmi flexa-waitfree-api-staging
  
.PHONY: build-prod
build-production: ## Build the production docker image.
	docker compose -f docker/production/compose.yaml build

.PHONY: start-prod
start-production: ## Start the production docker container.
	docker compose -f docker/production/compose.yaml up -d

.PHONY: stop-prod
stop-production: ## Stop the production docker container.
	docker compose -f docker/production/compose.yaml down && docker rmi flexa-waitfree-api-production