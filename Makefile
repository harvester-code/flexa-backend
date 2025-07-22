# Variables
PROFILE ?= flexa-waitfree-dev

# Functions
define start_server
	doppler setup -p flexa-waitfree-api -c $(1) && \
	aws-vault exec $(PROFILE) -- \
	doppler run -- fastapi $(2) app/main.py
endef

# Targets
start-localstack:
	docker-compose -f docker-compose.local.yml up -d

stop-localstack:
	docker-compose -f docker-compose.local.yml down

start-development:
	$(call start_server,dev,dev)

start-production:
	$(call start_server,prd,run)
