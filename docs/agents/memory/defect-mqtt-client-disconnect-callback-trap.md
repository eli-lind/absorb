---
title: MQTT Client Disconnect Callback Invocation Trap
category: defect
date: 2026-09-16
citations: 1
---

## Concrete Context
In `MqttRemoteService` (Issue #18), disconnect detection is implemented by hooking `client.onDisconnected`. When an unexpected network drop or broker shutdown occurs, `MqttRemoteService` receives the callback, updates status to `disconnected`, and initiates an exponential backoff auto-reconnection loop.
However, in the `mqtt_client` package, invoking explicit `client.disconnect()` triggers `internalDisconnect()`, which unconditionally executes `onDisconnected!()` if set.

## Lesson & Application
Calling `client.disconnect()` during deliberate user shutdown (e.g. turning off MQTT or calling `MqttRemoteService.disconnect()`) will fire `onDisconnected` and erroneously trigger an unexpected auto-reconnect loop unless explicitly unhooked.
Always clear `client.onDisconnected = null;` immediately before calling `client.disconnect()` in any MQTT client adapter.
