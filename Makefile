
all: test build

rabbitmq-server:
	 ./.ci/ubuntu/gha-setup.sh start pull

rabbitmq-server-stop:
	 ./.ci/ubuntu/gha-setup.sh stop

format:
	poetry run isort --skip rabbitmq_amqp_python_client/qpid  .
	poetry run black rabbitmq_amqp_python_client/
	poetry run black tests/
	poetry run flake8 --exclude=venv,local_tests,docs/examples,rabbitmq_amqp_python_client/qpid --max-line-length=120 --ignore=E203,W503

test: format
	poetry run pytest .
help:
	cat Makefile
