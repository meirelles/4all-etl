devenv:
	rm -rf .devenv
	virtualenv -p `which python3.6` .devenv
	( \
		. ./.devenv/bin/activate; \
		pip install -r airflow/requirements.txt; \
	)

# Just try run and create the containers if need so
run:
	docker-compose up

# Delete any existing container and create everything from scratch
zero_run:
	docker-compose down 2> /dev/null || true
	docker-compose up

# Run test command
tests: tests_clean
	COMPOSE_PROJECT_NAME=4all_etl_test docker-compose run --rm -e TEST=1 webserver

tests_clean:
	COMPOSE_PROJECT_NAME=4all_etl_test docker-compose down 2> /dev/null || true

# Clean all docker containers and images related to this project
clean: tests_clean
	docker-compose down -v --rmi all --remove-orphans
