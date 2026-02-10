# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Changed (breaking)
- **Consumer options aligned with uniform AMQP 1.0 clients interface** (cf. rabbitmq-amqp-dotnet-client [#144](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/144)):
  - Renamed `ConsumerFeature` to `ConsumerSettleStrategy`.
  - Renamed enum values: `DefaultSettle` → `ExplicitSettle`, `Presettled` → `PreSettled` (DirectReplyTo unchanged).
  - `ConsumerOptions` now takes `settle_strategy: ConsumerSettleStrategy` instead of `feature: ConsumerFeature`. Use `ConsumerOptions(settle_strategy=ConsumerSettleStrategy.ExplicitSettle)` (default), `ConsumerSettleStrategy.DirectReplyTo`, or `ConsumerSettleStrategy.PreSettled`.

## [[0.4.1](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.4.1)]

## 0.4.1 - 2025-01-14
- [Release 0.4.1](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.4.1)

### Added
- Implement consumer offset datetime by @Gsantomaggio in [#92](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/92)
- Bump urllib3 from 2.6.0 to 2.6.3 in [#93](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/93)

## [[0.3.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.3.0)]

## 0.3.0 - 2025-18-11
- [Release 0.3.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.3.0)

### Added
- Add asynchronous interface by @dadodimauro in [#86](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/86)

## [[0.4.0]](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.4.0)

## 0.4.0 - 2025-16-12
- [Release 0.4.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.4.0)

### Added
- Implement Direct Reply To Feature in [#87](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/87)
- Add and RPC example using direct reply queue in [#89](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/89)
- Bump urllib3 from 2.5.0 to 2.6.0 in [#90](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/90)

