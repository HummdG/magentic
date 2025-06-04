.PHONY: lint test build run

lint:            ## Ruff lint+format
	ruff check src tests --fix

test:            ## Run pytest
	pytest -q

build:           ## Build docker image
	docker build -t price-matcher .

run:             ## Run matcher inside container (expects CSVs mounted at ./data)
	docker run --rm -v $(PWD)/data:/data price-matcher \
	  --input /data/deliver_items/delivery_items_2025-06-04.csv
