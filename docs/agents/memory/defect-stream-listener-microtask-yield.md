---
title: Asynchronous Broadcast Stream Dispatch Requires Microtask Yield
category: defect
date: 2026-09-15
citations: 1
---

## Concrete Context
In unit tests for `MqttRemoteService` (Ticket #6), sending an inbound command via `fakeMqttClient.simulateInboundMessage('absorb/<slug>/set', 'STOP')` failed immediate verification (`verify(() => mockAudioPlayerService.stop()).called(1)`) with `No matching calls`.

## Lesson & Application
In Dart, broadcasting messages onto a `StreamController` schedule listener callbacks asynchronously on the event / microtask loop. Calling `fakeMqttClient.simulateInboundMessage(...)` does not synchronously trigger downstream listener invocations within the same call stack. Tests must yield to the event loop via `await Future<void>.delayed(Duration.zero);` before asserting method calls on mocks.
