---
title: MQTT Remote Control Protocol Contracts and Local Testing Topology
category: architecture
date: 2026-09-16
citations: 1
---

## Concrete Context
Building a standalone test harness (`tool/mqtt_harness.dart` and `scripts/test-mqtt`) to test the running app in an Android emulator revealed key protocol and environment constraints:
1. `absorb/<slug>/sleep_timer/set` silently failed to activate when sending raw strings (e.g. `'15'` or `'cancel'`); the service strictly parses JSON payloads (`{"duration_minutes": 15}`, `{"cancel": true}`, `{"mode": "end_of_chapter"}`).
2. Sleep timer telemetry state reports `mode: "time"` rather than `"timer"`, causing test assertion mismatches.
3. `absorb/<slug>/state` is published un-retained (`retain: false`), and periodic heartbeat telemetry runs exclusively while audio is actively playing (`isPlaying == true`).
4. Android emulators access host loopback services via `10.0.2.2`, requiring local test brokers (Mosquitto 2.0+) to explicitly bind `0.0.0.0` with `allow_anonymous true`.

## Lesson & Application
When testing or integrating with `MqttRemoteService`:
- Format all sleep timer set commands as JSON objects (`{"duration_minutes": N}`, `{"mode": "end_of_chapter"}`, `{"cancel": true}`).
- Match telemetry mode against `"time"`, `"end_of_chapter"`, and `"off"`.
- Never expect retained messages on `absorb/<slug>/state`; verify state via immediate command feedback or during active audio playback.
- For local emulator testing, set the app's MQTT broker host to `10.0.2.2` and run the host broker with `listener 1883 0.0.0.0`.
