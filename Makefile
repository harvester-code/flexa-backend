.PHONY: install test format lint dev run clean

PROJECT = flexa-simmula-api
CONFIG ?= dev

format:
	black app

dev:
	doppler run -p $(PROJECT) -c $(CONFIG) -- uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

run:
	doppler run -p $(PROJECT) -c prd -- uvicorn app.main:app --host 0.0.0.0 --port 8000

clean:
	@read -p "Are you sure you want to delete .venv? [y/N] " confirm; \
	if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
		echo "Removing .venv..."; \
		rm -rf .venv; \
	else \
		echo "Aborted."; \
	fi