---
title: Compound Download Keys for Podcast Episodes
category: architecture
date: 2026-09-15
citations: 1
---

## Concrete Context
During Ticket #5 (`play_media` handling in `MqttRemoteService`), checking `DownloadService.isDownloaded(itemId)` returned `false` for downloaded podcast episodes, causing unnecessary remote network calls instead of using local audio files.

## Lesson & Application
In Absorb, downloaded podcast episodes are keyed in `DownloadService` using a compound key formatted as `'$itemId-$episodeId'`, whereas standard audiobooks use `'$itemId'`. When querying or resolving downloaded items from `DownloadService` (e.g. `isDownloaded(key)` or `getDownloadedItem(key)`), code handling podcast episodes must construct the compound key whenever `episodeId` is present.
