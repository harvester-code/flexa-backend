start-localstack:
	docker-compose -f docker-compose.local.yml up -d

stop-localstack:
	docker-compose -f docker-compose.local.yml down

start-development:
	doppler setup -p flexa-waitfree-api -c dev && \
	doppler run -- fastapi dev app/main.py

start-production:
	doppler setup -p flexa-waitfree-api -c prd && \
	doppler run -- fastapi run app/main.py
