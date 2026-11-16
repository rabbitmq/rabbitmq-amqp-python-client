# Changelog

All notable changes to this project will be documented in this file.


## [Unreleased]

### Fixed
- Reconnect consumers after a dropped connection without creating a duplicate receiver, and start recovery only for an established connection whose transport closes unexpectedly.


## [[1.0.2](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v1.0.2)]

## 1.0.2 - 2026-11-16
- [Release 1.0.2](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v1.0.2)

### Added
- Optimize `encode_path_segment` in the address helper and fix Unicode encoding by @Pierpaolo1223 in [#154](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/154)

### Fixed
- Close the socket when the broker closes the connection first, and allow `close()` after a failed `dial()` by @adityaanikam in [#148](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/148)

### Changed
- Bump urllib3 from 2.7.0 to 2.8.0 by @dependabot in [#153](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/153)
- Bump pyjwt from 2.13.0 to 2.15.0 by @dependabot in [#152](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/152)
- Bump certifi from 2026.2.25 to 2026.10.18 by @dependabot in [#160](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/160)
- Bump black from 26.3.1 to 26.10.1 by @dependabot in [#161](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/161)


## [[1.0.1](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v1.0.1)]

## 1.0.1 - 2026-09-01
- [Release 1.0.1](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v1.0.1)

### Fixed
- Redact password in log statement by @greenbigfrog in [#130](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/130)


## [[1.0.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v1.0.0)]

## 1.0.0 - 2026-07-20
- [Release 1.0.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v1.0.0)

### Added
- Promote to the first stable release by @Gsantomaggio in [#122](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/122)

### Fixed
- Fix multiconnection loop not breaking after successful connect by @Chr1s70ph in [#120](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/120)

### Changed
- Bump pyjwt from 2.12.1 to 2.13.0 by @dependabot in [#121](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/121)


## [[0.8.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.8.0)]

## 0.8.0 - 2026-06-15
- [Release 0.8.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.8.0)

### Added
- Add `AmqpUri` class for structured AMQP connection configuration by @Gsantomaggio in [#113](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/113)
- Enforce peer verification for SSL connections by @MirahImage in [#115](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/115)
- Ensure `bytes_to_string` can handle non-ASCII encoded UTF-8 strings by @MirahImage in [#116](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/116)

### Changed
- Bump idna from 3.11 to 3.15 by @dependabot in [#112](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/112)


## [[0.7.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.7.0)]

## 0.7.0 - 2026-05-18
- [Release 0.7.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.7.0)

### Added
- Raise `AmqpMessageRejectedException` when a published message is rejected by the broker. The exception message contains the rejection reason provided by the broker (queue name and specific reason). This requires RabbitMQ 4.3+ to include detailed rejection information; older versions will raise the exception with a generic message. [#108](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/108)
- Add `QuorumConsumerOptions` with `sac_state_handler` callback for Quorum Queue Single Active Consumer (SAC) state-change notifications via AMQP 1.0 FLOW `rabbitmq:active` link-state property (RabbitMQ 4.3+). The callback receives `True` when the consumer becomes active and `False` when it is placed in standby. [#109](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/109)

### Changed
- Refresh declared dependency ranges (including dev tools) to current releases and fix stream consumer race condition by @Gsantomaggio in [#106](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/106)
- Bump pyjwt from 2.10.1 to 2.12.0 by @dependabot in [#105](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/105)

### Removed
- Remove `consume()` API from `Consumer` and `AsyncConsumer` by @Gsantomaggio in [#104](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/104). Use `message_handler` parameter with `connection.consumer()` instead.


## [[0.6.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.6.0)]

## 0.6.0 - 2026-03-17
- [Release 0.6.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.6.0)

### Changed
- Update packaging version to 24.2 by @TR0NZ0D in [#100](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/100)

## [[0.5.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.5.0)]

## 0.5.0 - 2025-02-16
- [Release 0.5.0](https://github.com/rabbitmq/rabbitmq-amqp-python-client/releases/tag/v0.5.0)

### Added
- Implement pre-settled by @Gsantomaggio in [#94](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/94)

### Changed
- Rename Consumer Feature to Consumer SettleStrategy by @Gsantomaggio in [#98](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/98)

### Deprecated
- Deprecate consume API by @Gsantomaggio in [#97](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/97). Use message handler instead of `consume()` API.

### Breaking changes
- Minor breaking change in [#98](https://github.com/rabbitmq/rabbitmq-amqp-python-client/pull/98): unify all the AMQP 1.0 clients' interfaces.

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

