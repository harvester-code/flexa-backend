# Targets
start-localstack:
	docker-compose -f docker-compose.local.yml up -d

stop-localstack:
	docker-compose -f docker-compose.local.yml down

start-development:
	doppler setup -p flexa-waitfree-api -c dev && \
	doppler run -- uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

start-production:
	doppler setup -p flexa-waitfree-api -c prd && \
	doppler run -- uvicorn app.main:app --host 0.0.0.0 --port 8000
