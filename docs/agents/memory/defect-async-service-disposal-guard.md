---
title: Async Service Disposal Guard for ChangeNotifier
category: defect
date: 2026-09-16
citations: 1
---

## Concrete Context
In `MqttRemoteService` (Ticket #19), handling connectivity restoration triggers asynchronous reconnection chains (`connectFromSettings`) that perform multiple awaits across settings queries and socket connections. When tests or lifecycle events invoke `dispose()` during an in-flight connection sequence, late-resuming microtasks invoked `_setConnectionStatus` and triggered `notifyListeners()` on the disposed `ChangeNotifier`, throwing `debugAssertNotDisposed`. Furthermore, setting `_isDisposed = true` before `disconnect()` prevented `disconnect()` from resetting status to `disconnected`.

## Lesson & Application
For services extending `ChangeNotifier` that manage asynchronous reconnection or background loops:
1. In `dispose()`, guard against double-disposal (`if (_isDisposed) return;`), invoke session disconnect, and only then set `_isDisposed = true;`.
2. Guard all status transition methods (`_setConnectionStatus`) and public async methods (`connect`, `connectFromSettings`, `_attemptReconnect`) with `if (_isDisposed) return;` to prevent post-disposal listener notifications.
