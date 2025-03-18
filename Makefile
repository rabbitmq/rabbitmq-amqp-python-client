
all: test build

rabbitmq-server:
	 ./.ci/ubuntu/gha-setup.sh start pull

rabbitmq-server-stop:
	 ./.ci/ubuntu/gha-setup.sh stop

help:
	cat Makefile
