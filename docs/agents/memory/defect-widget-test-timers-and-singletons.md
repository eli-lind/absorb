---
title: Widget Testing Traps with Singletons, Timers, and Lazy ListViews
category: defect
date: 2026-09-15
citations: 1
---

## Concrete Context
During Ticket #7, widget tests for `MqttSettingsScreen` timed out on `pumpAndSettle()`, failed to find offscreen action buttons, threw a `!timersPending` assertion failure at test tearDown due to background MQTT keepalive timers, and threw `MissingPluginException` for `PackageInfo`.

## Lesson & Application
1. **Singleton Service Injection**: Stateful UI screens that interact with singleton background services (`MqttRemoteService`, `AudioPlayerService`, etc.) should accept an optional instance via their constructor (e.g. `MqttSettingsScreen({this.mqttService})`). This allows widget tests to inject mock or fake services and prevents live background network clients from leaking persistent timers (like keepalive loops).
2. **Avoiding `pumpAndSettle()` with Active Animations**: Never call unbounded `pumpAndSettle()` when a widget tree displays infinite animations like `CircularProgressIndicator` (e.g. during connecting or loading states) or temporary overlay toasts. Use `await tester.pump()` with explicit durations (e.g. `tester.pump(const Duration(milliseconds: 100))`) and drain toast timers before test conclusion (`await tester.pump(const Duration(seconds: 4))`).
3. **Lazy `ListView` Viewports**: In Flutter widget tests, elements near the bottom of a `ListView` are lazily unbuilt if they fall outside the default 800x600 test surface. Either expand the test surface (`tester.view.physicalSize = const Size(1200, 1800)`) or use `await tester.ensureVisible(finder)` before tapping buttons.
4. **Mocking Platform Channels**: `BackupService` export methods invoke `PackageInfo.fromPlatform()`. Tests calling `BackupService.exportSettings()` must initialize `PackageInfo.setMockInitialValues(...)` during test setup to avoid `MissingPluginException`.
