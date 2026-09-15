# Staged Memory Index

- [Compound Download Keys for Podcast Episodes](./architecture-podcast-compound-download-keys.md) — DownloadService keys podcast episodes by compound itemId-episodeId rather than bare itemId.
- [Widget Testing Traps with Singletons, Timers, and Lazy ListViews](./defect-widget-test-timers-and-singletons.md) — Prevent timer leaks via singleton constructor injection, avoid pumpAndSettle with continuous spinners, and expand viewports for lazy ListViews.
- [Asynchronous Broadcast Stream Dispatch Requires Microtask Yield](./defect-stream-listener-microtask-yield.md) — (Promoted to docs/agents/conventions.md) Broadcast stream events require yielding to the event loop before asserting mock invocations.
- [MQTT Remote Control Protocol Contracts and Local Testing Topology](./architecture-mqtt-remote-control-contracts-and-testing.md) — MQTT sleep timer requires JSON payloads, reports mode 'time', state is un-retained without active playback, and emulator routes to host loopback via 10.0.2.2.
- [MQTT Client Disconnect Callback Invocation Trap](./defect-mqtt-client-disconnect-callback-trap.md) — mqtt_client package invokes onDisconnected on explicit disconnect(), requiring clearing callback before disconnect to prevent false auto-reconnect loops.
