# Codebase Conventions & Agent Guardrails

Key architectural facts and APIs for agents working in this repository.

## 1. Playback & Audio Services

- **Seeking**: Use `AudioPlayerService().seekTo(Duration pos, ...)` for player position seeks. Do not call `.seek()` on `AudioPlayerService` (which only exists on the low-level `AudioPlayerHandler`).
- **Item Resolution & Playback**: `AudioPlayerService().playItem(...)` requires resolved metadata (`api`, `title`, `author`, `duration`, `chapters`). Remote playback triggers (`play_media`) must resolve metadata via `DownloadService` (if downloaded offline) or `ApiService` before calling `playItem()`.

## 2. Sleep Timer Service

- **Timed Sleep**: Use `SleepTimerService().setTimeSleep(Duration duration)`.
- **End-of-Chapter Sleep**: Use `SleepTimerService().setChapterSleep(1)`.
- **Cancellation**: Use `SleepTimerService().cancel()`.
- (There are no `startTimer`, `startEndOfChapterTimer`, or `cancelTimer` methods).

## 3. Configuration & Preference Scoping (`ScopedPrefs`)

- `ScopedPrefs` scopes keys per logged-in user account.
- Any device-level or fleet configuration (e.g. MQTT broker host, port, credentials, device slug) **must** be registered in `ScopedPrefs._globalKeys` or written directly to `SharedPreferences` so it survives account switching, logouts, and pre-login application states.

## 4. Async Stream & Event Loop Testing

- **Broadcast Stream Microtask Yield**: In Dart, broadcasting events onto a `StreamController.broadcast()` schedules listener callbacks asynchronously on the microtask loop. Testing code that simulates inbound stream events (e.g. `simulateInboundMessage`) must yield to the event loop via `await Future<void>.delayed(Duration.zero);` or `async.flushMicrotasks()` (in `fakeAsync`) before asserting downstream mock invocations.
