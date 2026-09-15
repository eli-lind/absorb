# Staged Memory Index

- [Compound Download Keys for Podcast Episodes](./architecture-podcast-compound-download-keys.md) — DownloadService keys podcast episodes by compound itemId-episodeId rather than bare itemId.
- [Widget Testing Traps with Singletons, Timers, and Lazy ListViews](./defect-widget-test-timers-and-singletons.md) — Prevent timer leaks via singleton constructor injection, avoid pumpAndSettle with continuous spinners, and expand viewports for lazy ListViews.
- [Asynchronous Broadcast Stream Dispatch Requires Microtask Yield](./defect-stream-listener-microtask-yield.md) — Broadcast stream events require yielding to the event loop before asserting mock invocations.
