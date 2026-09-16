---
title: Mocktail Sequential Verification Invocations Counter Reset
category: defect
date: 2026-09-16
citations: 1
---

## Concrete Context
During unit testing for Home Assistant sleep timer discovery and duration slider commands (Ticket #22 in `test/services/mqtt_remote_service_test.dart`), test assertions simulated consecutive MQTT messages and verified mock service calls across distinct steps. Expecting `verify(() => mock.setDuration(any())).called(2)` after a second event failed because `verify().called(1)` on the first event consumed the unverified invocation counter.

## Lesson & Application
In `mocktail`, `verify(...).called(count)` asserts against *unverified* invocations recorded since the last `verify` call for that member, not cumulative lifetime calls:
- After calling `verify(...).called(1)` for an initial interaction, a subsequent `verify(...).called(1)` expects exactly one additional call since the first verification.
- To assert total calls at the conclusion of a test without consuming intermediary steps, defer `verify(...).called(total)` until all actions complete, or expect incremental call counts per step.
