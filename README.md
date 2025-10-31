## RabbitMQ AMQP 1.0 Python Client
This library is meant to be used with RabbitMQ `4.x`. Suitable for testing in pre-production environments.

The client is distributed via [`PIP`](https://pypi.org/project/rabbitmq-amqp-python-client/):
```bash
 pip install rabbitmq-amqp-python-client
```

### Getting Started    

Inside the [examples](./examples) folder you can find a set of examples that show how to use the client.


### Documentation

[Client Guide](https://www.rabbitmq.com/client-libraries/amqp-client-libraries) select the python section.


### Build

- `make rabbitmq-server`: run the RabbitMQ server in a docker container
- `poetry build`: build the source project
- `poetry install`: resolves and install dependencies
- `make test`: run the tests

### Note for MAC users:

To run TLS you need to:
``` bash
- pip uninstall python-qpid-proton
- brew install swig
- brew install pkg-config
- export CFLAGS="-I/usr/local/opt/openssl/include"; pip install python-qpid-proton --verbose --no-cache-dir
```

Read more about the issue [here](https://stackoverflow.com/questions/44979947/python-qpid-proton-for-mac-using-amqps)

### SSL Problems in local enviroment

If when running tests, this exceptions is raised by the proton library: `SSLUnavailable`:
``` bash
pip uninstall python-qpid-proton -y

sudo apt-get update
sudo apt-get install -y swig cmake build-essential libssl-dev pkg-config

export PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig
export CFLAGS="-I/usr/include/openssl"
export LDFLAGS="-L/usr/lib/x86_64-linux-gnu"

pip install "python-qpid-proton>=0.39.0,<0.40.0" --no-binary python-qpid-proton --verbose --no-cache-dir
```






