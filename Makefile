.PHONY: development-start
development-start:
	docker compose -p flexa-waitfree-api-dev -f docker/development/compose.yaml up -d

.PHONY: development-stop
development-stop:
	docker compose -p flexa-waitfree-api-dev -f docker/development/compose.yaml down

.PHONY: staging-start
staging-start:
	docker compose -p flexa-waitfree-api-stag -f docker/staging/compose.yaml up -d

.PHONY: staging-stop
staging-stop:
	docker compose -p flexa-waitfree-api-stag -f docker/staging/compose.yaml down

.PHONY: production-start
production-start:
	docker compose -p flexa-waitfree-api-prod -f docker/production/compose.yaml up -d

.PHONY: production-stop
production-stop:
	docker compose -p flexa-waitfree-api-prod -f docker/production/compose.yaml down
