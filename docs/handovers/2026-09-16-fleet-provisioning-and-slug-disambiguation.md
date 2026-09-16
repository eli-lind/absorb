# Handover: Fleet Provisioning and Slug Disambiguation

**Date**: 2026-09-16  
**Session Outcome**: Frontier Drained (Milestone 1 "MQTT Remote Control" Completed & Closed)  
**Ending**: Hand off (clean frontier boundary, zero unmerged WIP, zero open ready-for-agent tickets)

---

## 1. What Shipped

All workable tickets in the frontier have shipped and landed on `main`:

| Ticket | Scope | Role | Status / Target |
| :--- | :--- | :--- | :--- |
| [#42](https://github.com/eli-lind/absorb/issues/42) | SPEC: Fleet Provisioning and Device Slug Disambiguation | Spec Parent | Closed (Shipped) |
| [#43](https://github.com/eli-lind/absorb/issues/43) | Tracer Bullet: Unique Default Slug Generation and Store Persistence | Tracer Bullet | Closed ([PR #46](https://github.com/eli-lind/absorb/pull/46), commit [`732c9cb`](https://github.com/eli-lind/absorb/commit/732c9cb)) |
| [#44](https://github.com/eli-lind/absorb/issues/44) | Device-Local Slug Scoping in BackupService Fleet Provisioning | Vertical Slice | Closed ([PR #47](https://github.com/eli-lind/absorb/pull/47), commit [`5284f02`](https://github.com/eli-lind/absorb/commit/5284f02)) |

---

## 2. Implementation Highlights & Key Gotchas

1. **Entropy Slug Generation & Storage ([#43](https://github.com/eli-lind/absorb/issues/43))**:
   - Replaced static fallback slug (`absorb`) with dynamic entropy generation (`absorb_<4-hex>`) via `Random.secure()` in [`MqttSettings.getSlug()`](file:///Users/elilindner/Personal/absorb/lib/services/mqtt_settings.dart).
   - Slugs persist to `SharedPreferences` immediately upon first generation, ensuring identity stability across cold boots.
   - Preserves legacy explicit slugs and user-configured custom slugs verbatim without re-generating.
   - Removed dead constant `defaultSlug = 'absorb'`.

2. **In-Flight Call Deduplication ([#43](https://github.com/eli-lind/absorb/issues/43))**:
   - Prevented concurrent initialization races on fresh installations by latching onto `_slugFuture` during async disk reads/writes.
   - Cleared safely in a `finally` block and reset in `setSlug()` to prevent stale identity latching.

3. **Device-Local Slug Scoping on Archive Restore ([#44](https://github.com/eli-lind/absorb/issues/44))**:
   - Modified [`BackupService.importSettings`](file:///Users/elilindner/Personal/absorb/lib/services/backup_service.dart) to strip `MqttSettings.fieldSlug` from imported archive payloads prior to applying broker configurations via `MqttSettings.fromMap()`.
   - Allows a single master backup archive to provision credentials, TLS, and broker settings across a multi-device fleet without overwriting local device identities or creating MQTT broker entity collisions.
   - Preserves full export auditing and backward compatibility (`BackupService.exportSettings()` and `MqttSettings.toMap()` still serialize the local slug).

4. **Zero-Touch Auto-Connect on Restore ([#44](https://github.com/eli-lind/absorb/issues/44))**:
   - Maintained automatic reconnection trigger (`MqttRemoteService.connectFromSettings()`) when restoring backups where `enabled: true`, operating strictly under the preserved device-local slug.
   - Exposed [`MqttRemoteService.setMockInstance()`](file:///Users/elilindner/Personal/absorb/lib/services/mqtt_remote_service.dart) for clean mock-interception and verification in test harnesses.

---

## 3. Known Gaps & Parked Deferrals

No defects or regressions remain. Milestone 1 (*MQTT Remote Control*) has been closed. The following intentionally parked deferrals remain in `icebox`:

| Ticket | Reason Parked | Revisit Condition |
| :--- | :--- | :--- |
| [#45](https://github.com/eli-lind/absorb/issues/45) | DEFERRAL: Live Broker Slug Collision Detection and Telemetry Advisory | Revisit if fleet operators report accidental manual slug collisions on shared brokers. |
| [#40](https://github.com/eli-lind/absorb/issues/40) | DEFERRAL: Fuzzy Title Playback and Tangible Media Tag Mapping | Revisit when voice assistant or physical NFC card automations require plain-text title matching without UUIDs. |
| [#39](https://github.com/eli-lind/absorb/issues/39) | DEFERRAL: Battery and Device Power Telemetry Sensor Discovery | Revisit when bedside smart-plug charging cutoffs or platform-specific battery plugins are prioritized. |
| [#8](https://github.com/eli-lind/absorb/issues/8) | DEFERRAL: Tablet Hardware State Coordination (Audio Sink & Auto-Foreground) | Revisit if Fully Kiosk proves inadequate for foreground raising or audio sink detection. |

---

## 4. How to Operate

- **Run unit & widget test suite**:
  ```bash
  flutter test
  ```
  All 101 tests pass.

- **Run static analyzer**:
  ```bash
  flutter analyze lib test
  ```
  0 issues found across `lib/` and `test/`.

- **Run MQTT CLI Harness**:
  ```bash
  # Smoke test mode against live broker (requires running app/emulator)
  dart run tool/mqtt_harness.dart --host <broker-host> --slug <device-slug> --smoke

  # Interactive shell mode
  dart run tool/mqtt_harness.dart --host <broker-host> --slug <device-slug>
  ```

---

## 5. Verification Evidence

- `flutter test`: 101/101 passed cleanly.
- `flutter analyze lib test`: 0 lints/warnings/errors.
- Git tree clean, `origin/main` synchronized.

---

## 6. Suggested Next Steps

Milestone 1 (*MQTT Remote Control*) is 100% complete and closed. The frontier is drained with zero open `ready-for-agent` tickets.
To proceed with future development:
1. Promote an `icebox` deferral ([#45](https://github.com/eli-lind/absorb/issues/45), [#40](https://github.com/eli-lind/absorb/issues/40), [#39](https://github.com/eli-lind/absorb/issues/39), or [#8](https://github.com/eli-lind/absorb/issues/8)) to `ready-for-agent`.
2. Define Milestone 2 or run `/brainstorm` to specify new features.
