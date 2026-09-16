---
title: Dart num.tryParse Returns Non-Finite Values That Throw on toInt
category: defect
date: 2026-09-16
citations: 1
---

## Concrete Context
When implementing permissive sleep timer command parsing in `MqttRemoteService` (Ticket #21), untrusted string inputs were parsed using `num.tryParse(rawPayload)`. Inputs such as `"Infinity"` or `"-Infinity"` are successfully parsed by `num.tryParse` into `double.infinity` / `double.negativeInfinity`. Calling `.toInt()` or `.round()` on these non-finite doubles throws an unhandled `UnsupportedError: Cannot convert double.infinity to int`.

## Lesson & Application
When parsing untrusted string or numeric payloads where an integer duration or count is required:
1. Prefer `int.tryParse(rawPayload)` if integer-only values are expected.
2. If accepting decimal representations via `num.tryParse()`, explicitly verify `.isFinite` before invoking `.toInt()` or `.round()`:
   ```dart
   final val = num.tryParse(input);
   if (val != null && val.isFinite && val > 0) {
     final minutes = val.toInt();
     // ...
   }
   ```
