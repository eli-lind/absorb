# Handover: MQTT State Loop Closure and Library Telemetry

**Date**: 2026-09-16  
**Session Outcome**: Frontier Drained (all `ready-for-agent` tickets delivered)  
**Ending**: Hand off (clean frontier boundary, zero unmerged WIP)

---

## 1. What Shipped

All workable tickets in the frontier have shipped and landed on `main` in commit [`8bffea9`](https://github.com/eli-lind/absorb/commit/8bffea9):

| Ticket | Scope | Role | Status / Target |
| :--- | :--- | :--- | :--- |
| [#34](https://github.com/eli-lind/absorb/issues/34) | SPEC: MQTT State Loop Closure and Library Telemetry | Spec Parent | Closed (Shipped) |
| [#35](https://github.com/eli-lind/absorb/issues/35) | Disambiguate Sleep Timer Duration Unique ID and Add Slider State Feedback | Vertical Slice | Closed ([8bffea9](https://github.com/eli-lind/absorb/commit/8bffea9)) |
| [#36](https://github.com/eli-lind/absorb/issues/36) | Bidirectional State Feedback for Sleep Timer Preset and Playback Speed Select Entities | Vertical Slice | Closed ([8bffea9](https://github.com/eli-lind/absorb/commit/8bffea9)) |
| [#37](https://github.com/eli-lind/absorb/issues/37) | Downloaded Library Index Telemetry (`absorb/<slug>/downloaded_items`) | Vertical Slice | Closed ([8bffea9](https://github.com/eli-lind/absorb/commit/8bffea9)) |
| [#38](https://github.com/eli-lind/absorb/issues/38) | Test Harness Refresh for Extended Discovery Entities and Control Round-trips | Tracer / Verification | Closed ([8bffea9](https://github.com/eli-lind/absorb/commit/8bffea9)) |

---

## 2. Implementation Highlights & Key Gotchas

1. **Unique ID Disambiguation ([#35](https://github.com/eli-lind/absorb/issues/35))**:
   - Home Assistant discovery collided on `unique_id: absorb_${_slug}_sleep_timer` between the duration number entity and the duration sensor.
   - Updated the slider number entity unique ID to `absorb_${_slug}_sleep_timer_duration`.
   - Wired `state_topic: absorb/<slug>/sleep_timer` with Jinja2 template:
     `{{ (value_json.remaining_seconds / 60) | round(0) if value_json.active else 0 }}`.

2. **Bidirectional State Loop Closure ([#36](https://github.com/eli-lind/absorb/issues/36))**:
   - Eliminated optimistic state drift in Home Assistant dropdown selects.
   - Speed select entity now subscribes to `absorb/<slug>/state` via `{{ value_json.speed | string + 'x' }}`.
   - Sleep timer preset select entity subscribes to `absorb/<slug>/sleep_timer` via:
     `{{ 'end_of_chapter' if value_json.mode == 'end_of_chapter' else ((value_json.initial_minutes | string + 'm') if value_json.active else 'off') }}`.
   - In-app changes reflect in Home Assistant dashboards instantaneously.

3. **Retained Offline Library Telemetry ([#37](https://github.com/eli-lind/absorb/issues/37))**:
   - Publishes retained JSON on `absorb/<slug>/downloaded_items` with full item inventory (`item_id`, `episode_id`, `title`, `author`, `cover_url`).
   - Handles compound podcast download keys (`${itemId}_${episodeId}`) and extracts episode IDs properly.
   - Attaches reactive listener to `DownloadService.downloadEvents` on connect, detaching safely on disconnect/teardown.

4. **CLI Harness & Verification ([#38](https://github.com/eli-lind/absorb/issues/38))**:
   - `bin/mqtt_harness.dart` monitors all 6 discovery configs and library telemetry.
   - Interactive commands added: `speed`, `speed-preset`, `preset`, `duration`, `downloads`.
   - Smoke test suite asserts discovery contracts, round-trips sleep timer presets (15m -> off), and tests speed adjustments (1.25x).

---

## 3. Known Gaps & Parked Deferrals

No defects or unhandled regressions remain. The following intentionally parked deferrals remain in `icebox`:

| Ticket | Reason Parked | Revisit Condition |
| :--- | :--- | :--- |
| [#39](https://github.com/eli-lind/absorb/issues/39) | DEFERRAL: Battery and Device Power Telemetry Sensor Discovery | Revisit when bedside smart-plug charging cutoffs or platform-specific battery plugins are prioritized. |
| [#40](https://github.com/eli-lind/absorb/issues/40) | DEFERRAL: Fuzzy Title Playback and Tangible Media Tag Mapping | Revisit when voice assistant or physical NFC card automations require plain-text title matching without UUIDs. |
| [#8](https://github.com/eli-lind/absorb/issues/8) | DEFERRAL: Tablet Hardware State Coordination (Audio Sink & Auto-Foreground) | Revisit if Fully Kiosk proves inadequate for foreground raising or audio sink detection. |

---

## 4. How to Operate

- **Run unit & widget test suite**:
  ```bash
  flutter test
  ```
  All 92 tests pass.

- **Run static analyzer**:
  ```bash
  flutter analyze lib test
  ```
  0 issues found across `lib/` and `test/`.

- **Run MQTT CLI Harness**:
  ```bash
  # Smoke test mode against live broker
  dart bin/mqtt_harness.dart --host <broker-host> --slug <device-slug> --smoke

  # Interactive shell mode
  dart bin/mqtt_harness.dart --host <broker-host> --slug <device-slug>
  ```

---

## 5. Verification Evidence

- `flutter test`: 92/92 passed cleanly.
- `flutter analyze lib test`: 0 lints/warnings/errors.
- Git tree clean, `origin/main` synchronized.

---

## 6. Suggested Next Steps

The autonomous frontier is completely drained. No open `ready-for-agent` tickets exist on the backlog.
Any subsequent work requires:
1. Promoting an icebox deferral ([#39](https://github.com/eli-lind/absorb/issues/39) or [#40](https://github.com/eli-lind/absorb/issues/40)) to `ready-for-agent`.
2. Running `/brainstorm` or `build-the-frontier` to cut new specifications for upcoming milestones.
