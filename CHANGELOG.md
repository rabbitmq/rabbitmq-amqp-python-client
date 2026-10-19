# Changelog

All notable changes to the 2.x line of the RabbitMQ AMQP 1.0 Python client are documented in this file.
Release notes are taken from the [GitHub releases](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases).

> 🚨 Version 2.0 is **not compatible** with 1.x. Be careful before upgrading the library.
> See the [2.0 discussion](https://github.com/rabbitmq/rabbitmq-amqp-python-client/discussions/123)
> and the documentation in the [`v2_main` branch](https://github.com/rabbitmq/rabbitmq-amqp-python-client/tree/v2_main).

## [2.0.0-beta.2] - 2026-10-19

### Added
- Fuzzy tests ([#145](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/145))

### Fixed
- Session window violation caused by conflating delivery-id with transfer-id ([#142](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/142))
- `test_undecodable_deliveries_do_not_stall_the_consumer` test failure ([#144](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/144))

## [2.0.0-beta.1] - 2026-09-24

First beta of version 2.0.

### Added
- Dockerfile for `console_application.py` with externalized parameters ([#138](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/138))

### Changed
- Hand credit back when a message handler raises ([#139](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/139))
- Reject undecodable deliveries and hand their credit back ([#140](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/140))

### Fixed
- `initial_credits` not bounding unsettled deliveries ([#136](https://github.com/rabbitmq/rabbitmq-amqp-python-client/issues/136), [#137](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/137))

### New Contributors
- @pasmontesinos made their first contribution in [#139](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/139)

## [2.0.0-alpha.3] - 2026-09-08

Alpha release of version 2.0. No functional changes listed in the release notes.

## [2.0.0-alpha.2] - 2026-08-31

Alpha release of version 2.0. No functional changes listed in the release notes.

[2.0.0-beta.2]: https://github.com/rabbitmq/rabbitmq-amqp-python-client/compare/v2.0.0-beta.1...v2.0.0-beta.2
[2.0.0-beta.1]: https://github.com/rabbitmq/rabbitmq-amqp-python-client/compare/v2.0.0-alpha.3...v2.0.0-beta.1
[2.0.0-alpha.3]: https://github.com/rabbitmq/rabbitmq-amqp-python-client/compare/v2.0.0-alpha.2...v2.0.0-alpha.3
[2.0.0-alpha.2]: https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v2.0.0-alpha.2
