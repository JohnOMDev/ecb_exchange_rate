.PHONY: \
	build \
	deploy \
	run_local \
	test \
	test_api \
	logs \
	install \
	format \
	lint \

.DEFAULT_GOAL:=help

SHELL=bash


compose_file = compose.yaml
service = ecb_pipeline
image_name = $(service)
tag = $(shell poetry version -s)

run:
    uvicorn ecb_pipeline.app:app --proxy-header --host 0.0.0.0 --port 8080

test:
	pytest -cov=ecb_pipeline --cov-report=html

build:
	@echo "Building image $(service):$(tag) from $(compose_file)"
	docker compose -f docker/$(compose_file) build $(service)
	docker tag "$(image_name)":"$(tag)" "$(image_name)":latest

build_docker:
	@echo "Building image $(service):$(tag) from docker/Dockerfile"
    docker build -t ecb_pipeline:latest -f docker/Dockerfile .

monitoring:
	docker compose -f docker/monitor/uptrace.yaml up -d


run_docker: build
	IMAGE_TAG="$(tag)" IMAGE_NAME=$(image_name) docker compose -f docker/$(compose_file) stop
	IMAGE_TAG="$(tag)" IMAGE_NAME=$(image_name) docker compose -f docker/$(compose_file) up -d
	@echo "You can check now http://localhost:8080/docs"

stop_docker:
	IMAGE_TAG="$(tag)" IMAGE_NAME=$(image_name) docker compose -f docker/$(compose_file) stop


ps:
	IMAGE_TAG="$(tag)" IMAGE_NAME=$(image_name) docker compose -f docker/$(compose_file) ps


logs:
	IMAGE_TAG="$(tag)" IMAGE_NAME=$(image_name) docker compose -f docker/$(compose_file) logs $(service)

config:
	docker compose -f ${DOCKER_COMPOSE_FILE} config

install:
	pip install --upgrade pip &&\
		pip install flake8 pytest pytest_cov black bandit

format:
	black ecb_pipeline/s1/

test_s1:
	pytest tests/s1/s1_test.py::test_download_bremen_state_data_success
	pytest tests/s1/s1_test.py::test_get_cadastral_data

lint:
	flake8 ecb_pipeline/s1/ --count --select=E9,F63,F7,F82 --show-source --statistics
	flake8 ecb_pipeline/s1/ --count --exit-zero --max-complexity=10 --max-line-length=250 --statistics

securty:
	bandit -r ecb_pipeline/s1/ --tests B101, B301, B303, B602, B701

docker-compose:
	docker compose -f docker/docker-compose.yml up -d  --build