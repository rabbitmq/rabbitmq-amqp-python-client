# AGENTS.md - Guide for AI Agents

This document provides essential information for AI agents working with the RabbitMQ AMQP Python Client codebase.

## Project Overview

The RabbitMQ AMQP Python Client is a Python library for interacting with RabbitMQ 4.x using the AMQP 1.0 protocol. It provides both synchronous and asynchronous interfaces for publishing, consuming, and managing RabbitMQ resources.

## Key Components

### Core Classes

- **`Environment`**: Connection pooler that manages AMQP connections
- **`Connection`**: Main connection class for interacting with RabbitMQ
- **`Publisher`**: Publishes messages to RabbitMQ queues/exchanges
- **`Consumer`**: Consumes messages from RabbitMQ queues/exchanges
- **`Management`**: Manages RabbitMQ resources (queues, exchanges, bindings)

### Async Interface

Located in `rabbitmq_amqp_python_client/asyncio/`:
- **`AsyncEnvironment`**: Async version of Environment
- **`AsyncConnection`**: Async version of Connection
- **`AsyncPublisher`**: Async version of Publisher
- **`AsyncConsumer`**: Async version of Consumer
- **`AsyncManagement`**: Async version of Management

The async classes are facades that:
- Wrap synchronous classes
- Execute blocking operations in thread pool executors
- Use `asyncio.Lock` for thread safety
- Implement async context managers

### Entities and Options

Located in `rabbitmq_amqp_python_client/entities.py`:
- **`StreamConsumerOptions`**: Configuration for stream consumers (supports `offset_specification` as `OffsetSpecification`, `int`, or `datetime`)
- **`StreamFilterOptions`**: Filter options for stream consumers
- **`OffsetSpecification`**: Enum for stream offset positions (first, next, last, timestamp)
- **`ConsumerOptions`**: Configuration for FIFO (Classic/Quorum) consumers; uses `settle_strategy` (ConsumerSettleStrategy) for uniform AMQP 1.0 client interface
- **`ConsumerSettleStrategy`**: Enum for settle strategy: ExplicitSettle, DirectReplyTo, PreSettled
- Queue/Exchange specifications: `QuorumQueueSpecification`, `StreamSpecification`, `ExchangeSpecification`, etc.

## Project Structure

```
rabbitmq_amqp_python_client/
├── __init__.py              # Public API exports
├── address_helper.py        # Address validation and formatting
├── amqp_consumer_handler.py # Base message handler class
├── connection.py            # Connection management
├── consumer.py              # Message consumer implementation
├── publisher.py              # Message publisher implementation
├── management.py             # RabbitMQ management operations
├── environment.py             # Connection pooling
├── entities.py                # Data classes and options
├── exceptions.py              # Custom exceptions
├── options.py                 # Receiver options
├── queues.py                  # Queue specifications
├── ssl_configuration.py       # SSL/TLS configuration
├── utils.py                   # Utility functions
└── asyncio/                   # Async interface implementations
    ├── connection.py
    ├── consumer.py
    ├── publisher.py
    └── management.py

tests/
├── conftest.py               # Pytest fixtures and test helpers
├── test_consumer.py           # Consumer tests (includes stream consumer tests)
├── test_streams.py            # Stream-specific tests
├── test_connection.py          # Connection tests
├── test_publisher.py           # Publisher tests
├── test_management.py          # Management tests
└── asyncio/                   # Async tests

examples/
├── getting_started/           # Basic usage examples
├── streams/                   # Stream queue examples
├── stream_consumer_offset_datetime/  # Datetime offset example
└── ...                        # Other examples
```

## Testing Patterns

### Test Structure

1. **Fixtures**: Defined in `tests/conftest.py`
   - `connection`: Synchronous connection fixture
   - `environment`: Environment fixture
   - `connection_with_reconnect`: Connection with reconnection enabled

2. **Message Handlers**: Custom handlers in `tests/conftest.py`
   - `MyMessageHandlerAccept`: Accepts all messages
   - `MyMessageHandlerAcceptStreamOffset`: For stream offset testing
   - `MyMessageHandlerNoack`: No acknowledgment handler
   - `MyMessageHandlerDiscard`: Discards messages
   - `MyMessageHandlerRequeue`: Requeues messages

3. **Test Pattern**:
   ```python
   def test_feature(connection: Connection, environment: Environment) -> None:
       # Setup
       queue_name = "test-queue-name"
       management = connection.management()
       management.declare_queue(QueueSpecification(name=queue_name))
       
       # Test logic
       try:
           # ... test code ...
       except ConsumerTestException:
           pass  # Expected termination
       finally:
           # Cleanup
           consumer.close()
           management.delete_queue(queue_name)
           management.close()
   ```

### Stream Consumer Tests

When testing `StreamConsumerOptions` with datetime offset:
- Use `StreamSpecification` (not `QuorumQueueSpecification`)
- Capture datetime between message batches
- Verify only messages after the datetime are consumed
- Use `MyMessageHandlerDatetimeOffset` or similar custom handler

Example:
```python
# Publish messages before datetime
publish_messages(...)
time.sleep(2)
starting_from_here = datetime.now()
time.sleep(0.1)
# Publish messages after datetime
publish_messages(...)

consumer = connection.consumer(
    addr_queue,
    consumer_options=StreamConsumerOptions(
        offset_specification=starting_from_here
    )
)
```

## Code Style and Conventions

1. **Type Hints**: Always use type hints for function parameters and return types
2. **Docstrings**: Use Google-style docstrings for classes and public methods
3. **Imports**: Group imports: stdlib, third-party, local
4. **Error Handling**: Use custom exceptions from `exceptions.py`
5. **Logging**: Use module-level loggers: `logger = logging.getLogger(__name__)`

## Common Tasks

### Adding a New Feature

1. **Implementation**:
   - Add code to appropriate module in `rabbitmq_amqp_python_client/`
   - Export from `__init__.py` if it's part of the public API
   - Add type hints and docstrings

2. **Tests**:
   - Add tests in `tests/` directory
   - Follow existing test patterns
   - Use appropriate fixtures from `conftest.py`
   - Ensure proper cleanup (close connections, delete queues)

3. **Documentation**:
   - Update `CHANGELOG.md` with the new feature
   - Add examples if applicable
   - Update this file if patterns change

### Adding Stream Consumer Features

1. **Options**: Extend `StreamConsumerOptions` or `StreamFilterOptions` in `entities.py`
2. **Implementation**: Update `consumer.py` to handle new options
3. **Tests**: Add to `tests/test_consumer.py` or `tests/test_streams.py`
4. **Examples**: Add example in `examples/streams/` or similar

### Working with Datetime Offsets

The `StreamConsumerOptions` supports `offset_specification` as:
- `OffsetSpecification` enum (first, next, last, timestamp)
- `int` (numeric offset)
- `datetime` (timestamp-based offset)

When using `datetime`:
- The datetime is converted to milliseconds since epoch
- Only messages published after the datetime are consumed
- Use `datetime.now()` to capture the current time

## Important Files

- **`rabbitmq_amqp_python_client/entities.py`**: All data classes, options, and specifications
- **`rabbitmq_amqp_python_client/consumer.py`**: Consumer implementation (handles StreamConsumerOptions)
- **`tests/conftest.py`**: Test fixtures and helper classes
- **`tests/test_consumer.py`**: Consumer tests including stream consumer with datetime
- **`CHANGELOG.md`**: Release notes and changes

## Testing Best Practices

1. **Isolation**: Each test should be independent
2. **Cleanup**: Always clean up resources (queues, connections)
3. **Naming**: Use descriptive test names: `test_<feature>_<scenario>`
4. **Fixtures**: Use pytest fixtures for setup/teardown
5. **Assertions**: Use clear assertion messages
6. **Stream Tests**: Use `StreamSpecification` for stream queues, not `QuorumQueueSpecification`

## Error Handling

Common exceptions:
- `ArgumentOutOfRangeException`: Invalid argument values
- `ValidationCodeException`: Validation errors (e.g., version compatibility)
- `ConnectionClosed`: Connection was closed

## Async Interface Notes

- Async classes wrap synchronous classes
- Use `async with` for resource management
- All operations must be awaited
- Signal handling uses `asyncio.Event` and `loop.add_signal_handler`
- Thread safety is handled via `asyncio.Lock`

## Version Information

- Current version: Check `pyproject.toml` or `__init__.py`
- Changelog: See `CHANGELOG.md`
- Latest release: Check GitHub releases

## Quick Reference

### Creating a Stream Consumer with Datetime Offset

```python
from datetime import datetime
from rabbitmq_amqp_python_client import (
    AddressHelper,
    Connection,
    StreamConsumerOptions,
    StreamSpecification,
)

# Setup
management = connection.management()
management.declare_queue(StreamSpecification(name="my-stream"))
addr = AddressHelper.queue_address("my-stream")

# Capture datetime
starting_time = datetime.now()

# Create consumer
consumer = connection.consumer(
    addr,
    consumer_options=StreamConsumerOptions(
        offset_specification=starting_time
    )
)
```

### Running Tests

```bash
make test  # Run all tests
pytest tests/test_consumer.py  # Run specific test file
pytest tests/test_consumer.py::test_stream_consumer_offset_datetime  # Run specific test
```

## Notes for AI Agents

1. **Always check existing patterns**: Look at similar code before implementing
2. **Follow test structure**: Use fixtures and cleanup properly
3. **Type hints are required**: Don't skip type annotations
4. **Update CHANGELOG.md**: When adding features, update the changelog
5. **Stream vs Queue**: Use `StreamSpecification` for streams, `QuorumQueueSpecification` for regular queues
6. **Datetime handling**: Be careful with timing in tests - use `time.sleep()` appropriately
7. **Resource cleanup**: Always close consumers and delete queues in tests
