# GIF - Gold Intelligence Framework Makefile

SHELL := /bin/bash

.PHONY: help install pipeline dashboard docs docker-up docker-down clean

help:
	@echo "🏆 Gold Intelligence Framework - Command Center"
	@echo "Usage:"
	@echo "  make install      Install dependencies using uv"
	@echo "  make pipeline     Run the full ingestion and transformation pipeline"
	@echo "  make dashboard    Launch the Streamlit dashboard"
	@echo "  make docs         Generate and serve dbt documentation"
	@echo "  make docker-up    Start the full stack (Airflow, dbt, Dashboard) in Docker"
	@echo "  make docker-down  Stop all Docker services"
	@echo "  make clean        Remove temporary files and logs"

install:
	uv sync

pipeline:
	uv run python main.py

dashboard:
	uv run streamlit run dashboard.py

docs:
	chmod +x generate_docs.sh
	./generate_docs.sh
	cd gold_dbt && uv run dbt docs serve --port 8082

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

get-airflow-pass:
	@echo "🔑 Airflow Admin Password:"
	@docker exec data_engineering_project-airflow-1 cat /usr/local/airflow/standalone_admin_password.txt

clean:
	rm -rf logs/*.log
	rm -rf gold_dbt/target/
	rm -rf docs/
