
all: test build

rabbitmq-server:
	docker build -t rabbitmq-tls-test .
	docker run -it --rm --name rabbitmq-tls-test \
		-p 5672:5672 -p 5671:5671 -p 15672:15672 \
		-e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-rabbitmq_stream advertised_host localhost" \
		rabbitmq-tls-test

help:
	cat Makefile
