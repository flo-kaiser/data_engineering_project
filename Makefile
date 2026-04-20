# GIF - Gold Intelligence Framework Makefile

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
	./generate_docs.sh
	cd gold_dbt && uv run dbt docs serve --port 8082

docker-up:
	docker-compose up -d postgres airflow-init
	@echo "Waiting for database initialization..."
	@sleep 10
	docker-compose up -d

docker-down:
	docker-compose down

clean:
	rm -rf logs/*.log
	rm -rf gold_dbt/target/
	rm -rf docs/
