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

### Async Interface (Experimental)

The client provides an async interface via the `rabbitmq_amqp_python_client.asyncio` module. The async classes act as facades that:

- Wrap the corresponding synchronous classes
- Execute blocking operations in a thread pool executor using `run_in_executor`
- Coordinate concurrent access using `asyncio.Lock`
- Implement proper async context managers (`async with`) for resource management
- Maintain API compatibility with the synchronous version

**Key differences from the synchronous interface:**

1. Use `AsyncEnvironment` instead of `Environment`
2. All operations must be awaited with `await`
3. Use `async with` for resource management (connections, publishers, consumers, management)
4. Consumer signal handling uses `asyncio.Event` and `loop.add_signal_handler`

For a complete example showing proper consumer termination and signal handling, refer to:

- [examples/getting_started/getting_started_async.py](./examples/getting_started/getting_started_async.py)

Additional async examples are available in the [examples](./examples) folder:

- OAuth2: [examples/oauth/oAuth2_async.py](./examples/oauth/oAuth2_async.py)
- Reconnection: [examples/reconnection/reconnection_example_async.py](./examples/reconnection/reconnection_example_async.py)
- Streams: [examples/streams/example_with_streams_async.py](./examples/streams/example_with_streams_async.py)
- TLS: [examples/tls/tls_example_async.py](./examples/tls/tls_example_async.py)


