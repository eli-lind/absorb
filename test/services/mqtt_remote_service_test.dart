import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:fake_async/fake_async.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:absorb/services/mqtt_remote_service.dart';
import 'package:absorb/services/audio_player_service.dart';
import 'package:absorb/services/sleep_timer_service.dart';
import 'package:absorb/services/api_service.dart';
import 'package:absorb/services/download_service.dart';
import 'package:absorb/services/mqtt_settings.dart';



class MockAudioPlayerService extends Mock implements AudioPlayerService {}

class MockSleepTimerService extends Mock implements SleepTimerService {}

class MockApiService extends Mock implements ApiService {}

class MockDownloadService extends Mock implements DownloadService {}

class FakeMqttClientAdapter implements MqttClientAdapter {
  MqttConnectionConfig? lastConfig;
  bool _isConnected = false;
  bool shouldSucceed = true;

  final List<String> subscriptions = [];
  final List<PublishedMqttMessage> publishedMessages = [];
  final StreamController<({String topic, String payload})> _incomingController =
      StreamController<({String topic, String payload})>.broadcast();

  @override
  void Function()? onDisconnected;

  @override
  Stream<({String topic, String payload})> get incomingMessages =>
      _incomingController.stream;

  @override
  bool get isConnected => _isConnected;

  @override
  Future<bool> connect(MqttConnectionConfig config) async {
    lastConfig = config;
    if (!shouldSucceed) {
      _isConnected = false;
      return false;
    }
    _isConnected = true;
    return true;
  }

  @override
  void disconnect() {
    _isConnected = false;
  }

  @override
  void subscribe(String topic) {
    subscriptions.add(topic);
  }

  @override
  void publish(String topic, String payload, {bool retain = false}) {
    publishedMessages.add(
      PublishedMqttMessage(topic: topic, payload: payload, retain: retain),
    );
  }

  void simulateInboundMessage(String topic, String payload) {
    _incomingController.add((topic: topic, payload: payload));
  }

  void simulateDisconnect() {
    _isConnected = false;
    onDisconnected?.call();
  }

  void dispose() {
    _incomingController.close();
  }
}

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  setUpAll(() {
    registerFallbackValue(Duration.zero);
    registerFallbackValue(MockApiService());
  });

  group('MqttRemoteService Tracer Bullet', () {
    late MockAudioPlayerService mockAudioPlayerService;
    late FakeMqttClientAdapter fakeMqttClient;
    late MqttRemoteService service;

    setUp(() {
      mockAudioPlayerService = MockAudioPlayerService();
      fakeMqttClient = FakeMqttClientAdapter();
      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
      when(() => mockAudioPlayerService.nowPlayingTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentAuthor).thenReturn('');
      when(() => mockAudioPlayerService.currentCoverUrl).thenReturn(null);
      when(() => mockAudioPlayerService.totalDuration).thenReturn(0.0);
      when(() => mockAudioPlayerService.position).thenReturn(Duration.zero);
      when(() => mockAudioPlayerService.volume).thenReturn(1.0);
      when(() => mockAudioPlayerService.speed).thenReturn(1.0);
      when(() => mockAudioPlayerService.chapters).thenReturn([]);
      when(() => mockAudioPlayerService.currentChapter).thenReturn(null);
      when(() => mockAudioPlayerService.play(logDetail: any(named: 'logDetail'), fromUi: any(named: 'fromUi')))
          .thenAnswer((_) async {});
      when(() => mockAudioPlayerService.pause())
          .thenAnswer((_) async {});


      service = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayerService,
        clientAdapter: fakeMqttClient,
      );
    });

    tearDown(() {
      service.dispose();
      fakeMqttClient.dispose();
    });

    test('configures clean session and LWT to publish offline retained to absorb/<slug>/status', () async {
      await service.connect(
        host: '192.168.1.50',
        port: 1883,
        slug: 'kids_tablet',
      );

      final config = fakeMqttClient.lastConfig!;
      expect(config.host, equals('192.168.1.50'));
      expect(config.port, equals(1883));
      expect(config.cleanSession, isTrue);
      expect(config.willTopic, equals('absorb/kids_tablet/status'));
      expect(config.willMessage, equals('offline'));
      expect(config.willRetain, isTrue);
    });

    test('sanitizes slug and resolves TLS default port to 8883', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'Kids Room / Tablet #1!',
        useTls: true,
      );

      final config = fakeMqttClient.lastConfig!;
      expect(config.port, equals(8883));
      expect(config.useTls, isTrue);
      expect(service.slug, equals('kids_room___tablet__1_'));
      expect(config.willTopic, equals('absorb/kids_room___tablet__1_/status'));
    });

    test('publishes retained online to absorb/<slug>/status upon successful connection', () async {
      await service.connect(
        host: '192.168.1.50',
        port: 1883,
        slug: 'kids_tablet',
      );

      final statusMsg = fakeMqttClient.publishedMessages.firstWhere(
        (m) => m.topic == 'absorb/kids_tablet/status',
      );
      expect(statusMsg.payload, equals('online'));
      expect(statusMsg.retain, isTrue);
    });

    test('subscribes to absorb/<slug>/set upon connection', () async {
      await service.connect(
        host: '192.168.1.50',
        port: 1883,
        slug: 'kids_tablet',
      );

      expect(fakeMqttClient.subscriptions, contains('absorb/kids_tablet/set'));
    });

    test('dispatches PLAY command to AudioPlayerService with fromUi: false when received on absorb/<slug>/set', () async {
      await service.connect(
        host: '192.168.1.50',
        port: 1883,
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'PLAY');
      await Future<void>.delayed(Duration.zero);

      verify(() => mockAudioPlayerService.play(logDetail: any(named: 'logDetail'), fromUi: false)).called(1);
    });

    test('dispatches PAUSE command to AudioPlayerService when received on absorb/<slug>/set', () async {
      await service.connect(
        host: '192.168.1.50',
        port: 1883,
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'PAUSE');
      await Future<void>.delayed(Duration.zero);

      verify(() => mockAudioPlayerService.pause()).called(1);
    });

    test('emits state update to absorb/<slug>/state when playback state toggles between playing and paused', () async {
      await service.connect(
        host: '192.168.1.50',
        port: 1883,
        slug: 'kids_tablet',
      );

      // Transition to playing
      when(() => mockAudioPlayerService.isPlaying).thenReturn(true);
      service.onPlayerStateChanged();

      var stateMsg = fakeMqttClient.publishedMessages.lastWhere(
        (m) => m.topic == 'absorb/kids_tablet/state',
      );
      expect(jsonDecode(stateMsg.payload)['state'], equals('playing'));
      expect(stateMsg.retain, isFalse);

      // Transition to paused
      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
      service.onPlayerStateChanged();

      stateMsg = fakeMqttClient.publishedMessages.lastWhere(
        (m) => m.topic == 'absorb/kids_tablet/state',
      );
      expect(jsonDecode(stateMsg.payload)['state'], equals('paused'));
      expect(stateMsg.retain, isFalse);

    });
  });

  group('MqttRemoteService Ticket #3: Rich Telemetry & Controls', () {
    late MockAudioPlayerService mockAudioPlayerService;
    late FakeMqttClientAdapter fakeMqttClient;
    late MqttRemoteService service;

    setUp(() {
      SharedPreferences.setMockInitialValues({});
      mockAudioPlayerService = MockAudioPlayerService();
      fakeMqttClient = FakeMqttClientAdapter();

      when(() => mockAudioPlayerService.isPlaying).thenReturn(true);
      when(() => mockAudioPlayerService.nowPlayingTitle).thenReturn('Chapter 1: The Beginning');
      when(() => mockAudioPlayerService.currentTitle).thenReturn('The Great Audiobook');
      when(() => mockAudioPlayerService.currentAuthor).thenReturn('Author Name');
      when(() => mockAudioPlayerService.currentCoverUrl).thenReturn('https://example.com/cover.jpg');
      when(() => mockAudioPlayerService.totalDuration).thenReturn(3600.0);
      when(() => mockAudioPlayerService.position).thenReturn(const Duration(seconds: 120));
      when(() => mockAudioPlayerService.volume).thenReturn(0.8);
      when(() => mockAudioPlayerService.speed).thenReturn(1.25);
      when(() => mockAudioPlayerService.chapters).thenReturn([
        {'title': 'Intro', 'start': 0.0, 'end': 60.0},
        {'title': 'Chapter 1: The Beginning', 'start': 60.0, 'end': 600.0},
      ]);
      when(() => mockAudioPlayerService.currentChapter).thenReturn(
        {'title': 'Chapter 1: The Beginning', 'start': 60.0, 'end': 600.0},
      );
      when(() => mockAudioPlayerService.seekTo(any())).thenAnswer((_) async {});
      when(() => mockAudioPlayerService.setVolume(any())).thenAnswer((_) async {});
      when(() => mockAudioPlayerService.skipForward(any())).thenAnswer((_) async {});
      when(() => mockAudioPlayerService.skipBackward(any())).thenAnswer((_) async {});

      service = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayerService,
        clientAdapter: fakeMqttClient,
      );
    });

    tearDown(() {
      service.dispose();
      fakeMqttClient.dispose();
    });

    test('absorb/<slug>/state payload includes all specified rich telemetry fields serialized as JSON', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      final stateMsg = fakeMqttClient.publishedMessages.lastWhere(
        (m) => m.topic == 'absorb/kids_tablet/state',
      );
      final json = jsonDecode(stateMsg.payload) as Map<String, dynamic>;

      expect(json['state'], equals('playing'));
      expect(json['title'], equals('Chapter 1: The Beginning'));
      expect(json['author'], equals('Author Name'));
      expect(json['book'], equals('The Great Audiobook'));
      expect(json['chapter_title'], equals('Chapter 1: The Beginning'));
      expect(json['chapter_index'], equals(1));
      expect(json['duration_seconds'], equals(3600.0));
      expect(json['position_seconds'], equals(120.0));
      expect(json['volume'], equals(0.8));
      expect(json['speed'], equals(1.25));
      expect(json['cover_url'], equals('https://example.com/cover.jpg'));
    });

    test('subscribes to absorb/<slug>/seek/set and dispatches seekTo() with seconds', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      expect(fakeMqttClient.subscriptions, contains('absorb/kids_tablet/seek/set'));

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/seek/set', '250');
      await Future<void>.delayed(Duration.zero);

      verify(() => mockAudioPlayerService.seekTo(const Duration(seconds: 250))).called(1);
    });

    test('subscribes to absorb/<slug>/volume/set and dispatches setVolume() for 0.0-1.0 and 0-100 ranges', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      expect(fakeMqttClient.subscriptions, contains('absorb/kids_tablet/volume/set'));

      // Float 0.0 - 1.0 range
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/volume/set', '0.65');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.setVolume(0.65)).called(1);

      // Percentage 0 - 100 range
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/volume/set', '75');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.setVolume(0.75)).called(1);
    });

    test('subscribes to absorb/<slug>/set and supports SKIP_FORWARD and SKIP_BACKWARD', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'SKIP_FORWARD');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.skipForward(30)).called(1);

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'SKIP_BACKWARD');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.skipBackward(10)).called(1);
    });


    test('throttles position heartbeat to 10-second interval during active playback', () async {
      fakeAsync((async) {
        service.connect(
          host: '192.168.1.50',
          slug: 'kids_tablet',
        );
        async.flushMicrotasks();

        final initialCount = fakeMqttClient.publishedMessages
            .where((m) => m.topic == 'absorb/kids_tablet/state')
            .length;

        // Advance 9 seconds: no new heartbeat
        async.elapse(const Duration(seconds: 9));
        expect(
          fakeMqttClient.publishedMessages
              .where((m) => m.topic == 'absorb/kids_tablet/state')
              .length,
          equals(initialCount),
        );

        // Advance 1 more second (10s total): heartbeat emitted
        async.elapse(const Duration(seconds: 1));
        expect(
          fakeMqttClient.publishedMessages
              .where((m) => m.topic == 'absorb/kids_tablet/state')
              .length,
          equals(initialCount + 1),
        );

        // Advance another 10 seconds: second heartbeat emitted
        async.elapse(const Duration(seconds: 10));
        expect(
          fakeMqttClient.publishedMessages
              .where((m) => m.topic == 'absorb/kids_tablet/state')
              .length,
          equals(initialCount + 2),
        );
      });
    });
  });

  group('MqttRemoteService Ticket #4: Dual-Mode Sleep Timer Control and Telemetry', () {
    late MockAudioPlayerService mockAudioPlayerService;
    late MockSleepTimerService mockSleepTimerService;
    late FakeMqttClientAdapter fakeMqttClient;
    late MqttRemoteService service;
    late List<VoidCallback> sleepTimerListeners;

    setUp(() {
      SharedPreferences.setMockInitialValues({});
      mockAudioPlayerService = MockAudioPlayerService();
      mockSleepTimerService = MockSleepTimerService();
      fakeMqttClient = FakeMqttClientAdapter();
      sleepTimerListeners = [];

      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
      when(() => mockAudioPlayerService.nowPlayingTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentAuthor).thenReturn('');
      when(() => mockAudioPlayerService.currentCoverUrl).thenReturn(null);
      when(() => mockAudioPlayerService.totalDuration).thenReturn(0.0);
      when(() => mockAudioPlayerService.position).thenReturn(Duration.zero);
      when(() => mockAudioPlayerService.volume).thenReturn(1.0);
      when(() => mockAudioPlayerService.speed).thenReturn(1.0);
      when(() => mockAudioPlayerService.chapters).thenReturn([]);
      when(() => mockAudioPlayerService.currentChapter).thenReturn(null);

      when(() => mockSleepTimerService.isActive).thenReturn(false);
      when(() => mockSleepTimerService.mode).thenReturn(SleepTimerMode.off);
      when(() => mockSleepTimerService.timeRemaining).thenReturn(Duration.zero);
      when(() => mockSleepTimerService.initialDuration).thenReturn(Duration.zero);
      when(() => mockSleepTimerService.addListener(any())).thenAnswer((invocation) {
        final listener = invocation.positionalArguments[0] as VoidCallback;
        sleepTimerListeners.add(listener);
      });
      when(() => mockSleepTimerService.removeListener(any())).thenAnswer((invocation) {
        final listener = invocation.positionalArguments[0] as VoidCallback;
        sleepTimerListeners.remove(listener);
      });
      when(() => mockSleepTimerService.setTimeSleep(any())).thenReturn(null);
      when(() => mockSleepTimerService.setChapterSleep(any())).thenReturn(null);
      when(() => mockSleepTimerService.cancel()).thenReturn(null);

      service = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayerService,
        sleepTimerService: mockSleepTimerService,
        clientAdapter: fakeMqttClient,
      );
    });

    tearDown(() {
      service.dispose();
      fakeMqttClient.dispose();
    });

    test('subscribes to absorb/<slug>/sleep_timer/set and publishes initial retained sleep timer state on connect', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      expect(fakeMqttClient.subscriptions, contains('absorb/kids_tablet/sleep_timer/set'));

      final sleepMsg = fakeMqttClient.publishedMessages.firstWhere(
        (m) => m.topic == 'absorb/kids_tablet/sleep_timer',
      );
      expect(sleepMsg.retain, isTrue);

      final payload = jsonDecode(sleepMsg.payload) as Map<String, dynamic>;
      expect(payload['active'], isFalse);
      expect(payload['mode'], equals('off'));
      expect(payload['remaining_seconds'], equals(0));
      expect(payload['initial_minutes'], equals(0));
    });

    test('publishes updated retained state when SleepTimerService changes', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      when(() => mockSleepTimerService.isActive).thenReturn(true);
      when(() => mockSleepTimerService.mode).thenReturn(SleepTimerMode.time);
      when(() => mockSleepTimerService.timeRemaining).thenReturn(const Duration(minutes: 19, seconds: 45));
      when(() => mockSleepTimerService.initialDuration).thenReturn(const Duration(minutes: 20));

      for (final listener in sleepTimerListeners) {
        listener();
      }

      final sleepMsg = fakeMqttClient.publishedMessages.lastWhere(
        (m) => m.topic == 'absorb/kids_tablet/sleep_timer',
      );
      expect(sleepMsg.retain, isTrue);

      final payload = jsonDecode(sleepMsg.payload) as Map<String, dynamic>;
      expect(payload['active'], isTrue);
      expect(payload['mode'], equals('time'));
      expect(payload['remaining_seconds'], equals(1185));
      expect(payload['initial_minutes'], equals(20));
    });

    test('parses duration minute requests on absorb/<slug>/sleep_timer/set and delegates to setTimeSleep', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        jsonEncode({'duration_minutes': 25}),
      );
      await Future<void>.delayed(Duration.zero);

      verify(() => mockSleepTimerService.setTimeSleep(const Duration(minutes: 25))).called(1);
    });

    test('parses end_of_chapter mode request on absorb/<slug>/sleep_timer/set and delegates to setChapterSleep(1)', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        jsonEncode({'mode': 'end_of_chapter'}),
      );
      await Future<void>.delayed(Duration.zero);

      verify(() => mockSleepTimerService.setChapterSleep(1)).called(1);
    });

    test('parses cancel request on absorb/<slug>/sleep_timer/set and delegates to cancel()', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        jsonEncode({'cancel': true}),
      );
      await Future<void>.delayed(Duration.zero);

      verify(() => mockSleepTimerService.cancel()).called(1);
    });

    test('throttles countdown ticks to 10-second intervals and emits transitions immediately', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      // Start a 15-minute timer -> transition from off to time emits immediately
      when(() => mockSleepTimerService.isActive).thenReturn(true);
      when(() => mockSleepTimerService.mode).thenReturn(SleepTimerMode.time);
      when(() => mockSleepTimerService.initialDuration).thenReturn(const Duration(minutes: 15));
      when(() => mockSleepTimerService.timeRemaining).thenReturn(const Duration(seconds: 900));

      for (final listener in sleepTimerListeners) {
        listener();
      }

      final count = fakeMqttClient.publishedMessages
          .where((m) => m.topic == 'absorb/kids_tablet/sleep_timer')
          .length;

      // 1-second tick (899s): diff is 1s (< 10s), no new message emitted
      when(() => mockSleepTimerService.timeRemaining).thenReturn(const Duration(seconds: 899));
      for (final listener in sleepTimerListeners) {
        listener();
      }
      expect(
        fakeMqttClient.publishedMessages
            .where((m) => m.topic == 'absorb/kids_tablet/sleep_timer')
            .length,
        equals(count),
      );

      // 10-second diff (890s): emits updated message
      when(() => mockSleepTimerService.timeRemaining).thenReturn(const Duration(seconds: 890));
      for (final listener in sleepTimerListeners) {
        listener();
      }
      expect(
        fakeMqttClient.publishedMessages
            .where((m) => m.topic == 'absorb/kids_tablet/sleep_timer')
            .length,
        equals(count + 1),
      );

      // Cancellation transition -> emits immediately regardless of throttle
      when(() => mockSleepTimerService.isActive).thenReturn(false);
      when(() => mockSleepTimerService.mode).thenReturn(SleepTimerMode.off);
      when(() => mockSleepTimerService.timeRemaining).thenReturn(Duration.zero);
      when(() => mockSleepTimerService.initialDuration).thenReturn(Duration.zero);
      for (final listener in sleepTimerListeners) {
        listener();
      }
      expect(
        fakeMqttClient.publishedMessages
            .where((m) => m.topic == 'absorb/kids_tablet/sleep_timer')
            .length,
        equals(count + 2),
      );
    });
  });

  group('MqttRemoteService Ticket #5: Chapter Navigation and Direct Item Playback', () {
    late MockAudioPlayerService mockAudioPlayerService;
    late MockApiService mockApiService;
    late MockDownloadService mockDownloadService;
    late FakeMqttClientAdapter fakeMqttClient;
    late MqttRemoteService service;

    setUp(() {
      SharedPreferences.setMockInitialValues({});
      mockAudioPlayerService = MockAudioPlayerService();
      mockApiService = MockApiService();
      mockDownloadService = MockDownloadService();
      fakeMqttClient = FakeMqttClientAdapter();

      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
      when(() => mockAudioPlayerService.nowPlayingTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentAuthor).thenReturn('');
      when(() => mockAudioPlayerService.currentCoverUrl).thenReturn(null);
      when(() => mockAudioPlayerService.totalDuration).thenReturn(0.0);
      when(() => mockAudioPlayerService.position).thenReturn(Duration.zero);
      when(() => mockAudioPlayerService.volume).thenReturn(1.0);
      when(() => mockAudioPlayerService.speed).thenReturn(1.0);
      when(() => mockAudioPlayerService.chapters).thenReturn([]);
      when(() => mockAudioPlayerService.currentChapter).thenReturn(null);
      when(() => mockAudioPlayerService.currentApi).thenReturn(mockApiService);
      when(() => mockAudioPlayerService.skipToNextChapter()).thenAnswer((_) async {});
      when(() => mockAudioPlayerService.skipToPreviousChapter()).thenAnswer((_) async {});
      when(() => mockAudioPlayerService.playItem(
            api: any(named: 'api'),
            itemId: any(named: 'itemId'),
            title: any(named: 'title'),
            author: any(named: 'author'),
            coverUrl: any(named: 'coverUrl'),
            totalDuration: any(named: 'totalDuration'),
            chapters: any(named: 'chapters'),
            episodeId: any(named: 'episodeId'),
            episodeTitle: any(named: 'episodeTitle'),
            libraryId: any(named: 'libraryId'),
          )).thenAnswer((_) async => null);

      when(() => mockDownloadService.isDownloaded(any())).thenReturn(false);

      service = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayerService,
        downloadService: mockDownloadService,
        apiProvider: () => mockApiService,
        clientAdapter: fakeMqttClient,
      );
    });

    tearDown(() {
      service.dispose();
      fakeMqttClient.dispose();
    });

    test('absorb/<slug>/set handles NEXT_CHAPTER and PREV_CHAPTER', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'NEXT_CHAPTER');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.skipToNextChapter()).called(1);

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'PREV_CHAPTER');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.skipToPreviousChapter()).called(1);
    });

    test('subscribes to absorb/<slug>/play_media/set on connect', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      expect(fakeMqttClient.subscriptions, contains('absorb/kids_tablet/play_media/set'));
    });

    test('parses play_media JSON, fetches via ApiService, and dispatches to playItem', () async {
      when(() => mockApiService.getLibraryItem('item-book-1')).thenAnswer((_) async => {
            'id': 'item-book-1',
            'libraryId': 'lib-1',
            'media': {
              'metadata': {
                'title': 'The Hobbit',
                'authorName': 'J.R.R. Tolkien',
              },
              'duration': 36000.0,
              'chapters': [
                {'title': 'An Unexpected Party', 'start': 0.0, 'end': 3000.0}
              ],
            },
          });
      when(() => mockApiService.getCoverUrl('item-book-1', width: 400))
          .thenReturn('https://abs.local/cover.jpg');

      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/play_media/set',
        jsonEncode({'item_id': 'item-book-1'}),
      );
      await Future<void>.delayed(Duration.zero);

      verify(() => mockAudioPlayerService.playItem(
            api: mockApiService,
            itemId: 'item-book-1',
            title: 'The Hobbit',
            author: 'J.R.R. Tolkien',
            coverUrl: 'https://abs.local/cover.jpg',
            totalDuration: 36000.0,
            chapters: [
              {'title': 'An Unexpected Party', 'start': 0.0, 'end': 3000.0}
            ],
            libraryId: 'lib-1',
          )).called(1);
    });

    test('parses play_media with optional episode_id and resolves episode', () async {
      when(() => mockApiService.getLibraryItem('show-123')).thenAnswer((_) async => {
            'id': 'show-123',
            'libraryId': 'lib-podcasts',
            'media': {
              'metadata': {
                'title': 'Podcast Show',
                'authorName': 'Host Name',
              },
              'episodes': [
                {
                  'id': 'ep-999',
                  'title': 'Episode 10: Special Guest',
                  'duration': 1800.0,
                  'chapters': [],
                }
              ],
            },
          });
      when(() => mockApiService.getCoverUrl('show-123', width: 400))
          .thenReturn('https://abs.local/podcast.jpg');

      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/play_media/set',
        jsonEncode({'item_id': 'show-123', 'episode_id': 'ep-999'}),
      );
      await Future<void>.delayed(Duration.zero);

      verify(() => mockAudioPlayerService.playItem(
            api: mockApiService,
            itemId: 'show-123',
            title: 'Episode 10: Special Guest',
            author: 'Podcast Show',
            coverUrl: 'https://abs.local/podcast.jpg',
            totalDuration: 1800.0,
            chapters: [],
            episodeId: 'ep-999',
            episodeTitle: 'Episode 10: Special Guest',
            libraryId: 'lib-podcasts',
          )).called(1);
    });

    test('resolves metadata from DownloadService when item is downloaded', () async {
      when(() => mockDownloadService.isDownloaded('downloaded-book-42')).thenReturn(true);
      when(() => mockDownloadService.getInfo('downloaded-book-42')).thenReturn(DownloadInfo(
        itemId: 'downloaded-book-42',
        title: 'Downloaded Title',
        author: 'Downloaded Author',
        coverUrl: 'file:///local/cover.jpg',
        libraryId: 'lib-downloaded',
        sessionData: jsonEncode({
          'duration': 7200.0,
          'chapters': [
            {'title': 'Chapter 1', 'start': 0.0, 'end': 7200.0}
          ],
        }),
      ));

      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/play_media/set',
        jsonEncode({'item_id': 'downloaded-book-42'}),
      );
      await Future<void>.delayed(Duration.zero);

      verifyNever(() => mockApiService.getLibraryItem(any()));
      verify(() => mockAudioPlayerService.playItem(
            api: mockApiService,
            itemId: 'downloaded-book-42',
            title: 'Downloaded Title',
            author: 'Downloaded Author',
            coverUrl: 'file:///local/cover.jpg',
            totalDuration: 7200.0,
            chapters: [
              {'title': 'Chapter 1', 'start': 0.0, 'end': 7200.0}
            ],
            libraryId: 'lib-downloaded',
          )).called(1);
    });

    test('rejects malformed play_media payloads gracefully without crashing', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      // Empty payload
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/play_media/set', '');
      await Future<void>.delayed(Duration.zero);

      // Non-JSON string
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/play_media/set', 'not-json');
      await Future<void>.delayed(Duration.zero);

      // JSON array
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/play_media/set', '["item-1"]');
      await Future<void>.delayed(Duration.zero);

      // Missing item_id
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/play_media/set', '{"foo": "bar"}');
      await Future<void>.delayed(Duration.zero);

      // Empty item_id
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/play_media/set', '{"item_id": " "}');
      await Future<void>.delayed(Duration.zero);

      verifyNever(() => mockAudioPlayerService.playItem(
            api: any(named: 'api'),
            itemId: any(named: 'itemId'),
            title: any(named: 'title'),
            author: any(named: 'author'),
            coverUrl: any(named: 'coverUrl'),
            totalDuration: any(named: 'totalDuration'),
            chapters: any(named: 'chapters'),
          ));
    });

    test('resolves downloaded episode using compound itemId-episodeId key', () async {
      when(() => mockDownloadService.isDownloaded('show-pod-1-ep-99')).thenReturn(true);
      when(() => mockDownloadService.getInfo('show-pod-1-ep-99')).thenReturn(DownloadInfo(
        itemId: 'show-pod-1-ep-99',
        title: 'Downloaded Episode Title',
        author: 'Show Author',
        coverUrl: 'file:///local/show.jpg',
        libraryId: 'lib-podcasts',
        sessionData: jsonEncode({
          'duration': 1200.0,
          'chapters': [],
        }),
      ));

      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/play_media/set',
        jsonEncode({'item_id': 'show-pod-1', 'episode_id': 'ep-99'}),
      );
      await Future<void>.delayed(Duration.zero);

      verifyNever(() => mockApiService.getLibraryItem(any()));
      verify(() => mockAudioPlayerService.playItem(
            api: mockApiService,
            itemId: 'show-pod-1',
            title: 'Downloaded Episode Title',
            author: 'Show Author',
            coverUrl: 'file:///local/show.jpg',
            totalDuration: 1200.0,
            chapters: [],
            episodeId: 'ep-99',
            episodeTitle: 'Downloaded Episode Title',
            libraryId: 'lib-podcasts',
          )).called(1);
    });

    test('gracefully rejects when requested episode_id is not found on item', () async {
      when(() => mockApiService.getLibraryItem('show-missing-ep')).thenAnswer((_) async => {
            'id': 'show-missing-ep',
            'media': {
              'metadata': {'title': 'Some Show', 'authorName': 'Author'},
              'episodes': [
                {'id': 'ep-1', 'title': 'Ep 1'}
              ],
            },
          });
      when(() => mockApiService.getCoverUrl(any(), width: any(named: 'width')))
          .thenReturn('https://abs.local/cover.jpg');

      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/play_media/set',
        jsonEncode({'item_id': 'show-missing-ep', 'episode_id': 'ep-nonexistent'}),
      );
      await Future<void>.delayed(Duration.zero);

      verifyNever(() => mockAudioPlayerService.playItem(
            api: any(named: 'api'),
            itemId: any(named: 'itemId'),
            title: any(named: 'title'),
            author: any(named: 'author'),
            coverUrl: any(named: 'coverUrl'),
            totalDuration: any(named: 'totalDuration'),
            chapters: any(named: 'chapters'),
          ));
    });
  });

  group('MqttRemoteService Ticket #6: Home Assistant MQTT Discovery', () {
    late MockAudioPlayerService mockAudioPlayerService;
    late FakeMqttClientAdapter fakeMqttClient;
    late MqttRemoteService service;

    setUp(() {
      SharedPreferences.setMockInitialValues({});
      mockAudioPlayerService = MockAudioPlayerService();
      fakeMqttClient = FakeMqttClientAdapter();

      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
      when(() => mockAudioPlayerService.nowPlayingTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentAuthor).thenReturn('');
      when(() => mockAudioPlayerService.currentCoverUrl).thenReturn(null);
      when(() => mockAudioPlayerService.totalDuration).thenReturn(0.0);
      when(() => mockAudioPlayerService.position).thenReturn(Duration.zero);
      when(() => mockAudioPlayerService.volume).thenReturn(1.0);
      when(() => mockAudioPlayerService.speed).thenReturn(1.0);
      when(() => mockAudioPlayerService.chapters).thenReturn([]);
      when(() => mockAudioPlayerService.currentChapter).thenReturn(null);

      service = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayerService,
        clientAdapter: fakeMqttClient,
      );
    });

    tearDown(() {
      service.dispose();
      fakeMqttClient.dispose();
    });

    test('publishes retained media_player discovery configuration on connect', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
        enableDiscovery: true,
      );

      final discoveryMsg = fakeMqttClient.publishedMessages.firstWhere(
        (m) => m.topic == 'homeassistant/media_player/absorb_kids_tablet/config',
      );
      expect(discoveryMsg.retain, isTrue);

      final payload = jsonDecode(discoveryMsg.payload) as Map<String, dynamic>;
      expect(payload['unique_id'], equals('absorb_kids_tablet_media_player'));
      expect(payload['state_topic'], equals('absorb/kids_tablet/state'));
      expect(payload['command_topic'], equals('absorb/kids_tablet/set'));
      expect(payload['seek_topic'], equals('absorb/kids_tablet/seek/set'));
      expect(payload['volume_command_topic'], equals('absorb/kids_tablet/volume/set'));
      expect(payload['availability_topic'], equals('absorb/kids_tablet/status'));
      expect(payload['payload_available'], equals('online'));
      expect(payload['payload_not_available'], equals('offline'));
      expect(payload['payload_play'], equals('PLAY'));
      expect(payload['payload_pause'], equals('PAUSE'));
      expect(payload['payload_stop'], equals('STOP'));
      expect(payload['payload_next_track'], equals('NEXT_CHAPTER'));
      expect(payload['payload_previous_track'], equals('PREV_CHAPTER'));

      final features = List<String>.from(payload['supported_features'] as List);
      expect(features, containsAll([
        'play',
        'pause',
        'stop',
        'seek',
        'volume_set',
        'next_track',
        'previous_track',
        'play_media',
      ]));

      final device = payload['device'] as Map<String, dynamic>;
      expect(device['identifiers'], contains('absorb_kids_tablet'));
      expect(device['name'], equals('Absorb (kids_tablet)'));
      expect(device['model'], isNotEmpty);
      expect(device['manufacturer'], isNotEmpty);
      expect(device['sw_version'], isNotEmpty);
    });

    test('publishes retained sleep timer sensor discovery configuration on connect', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
        enableDiscovery: true,
      );

      final discoveryMsg = fakeMqttClient.publishedMessages.firstWhere(
        (m) => m.topic == 'homeassistant/sensor/absorb_kids_tablet_sleep_timer/config',
      );
      expect(discoveryMsg.retain, isTrue);

      final payload = jsonDecode(discoveryMsg.payload) as Map<String, dynamic>;
      expect(payload['unique_id'], equals('absorb_kids_tablet_sleep_timer'));
      expect(payload['state_topic'], equals('absorb/kids_tablet/sleep_timer'));
      expect(payload['value_template'], equals('{{ value_json.remaining_seconds }}'));
      expect(payload['unit_of_measurement'], equals('s'));
      expect(payload['device_class'], equals('duration'));
      expect(payload['availability_topic'], equals('absorb/kids_tablet/status'));

      final device = payload['device'] as Map<String, dynamic>;
      expect(device['identifiers'], contains('absorb_kids_tablet'));
      expect(device['name'], equals('Absorb (kids_tablet)'));
    });

    test('publishes retained end-of-chapter sleep button discovery configuration on connect', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
        enableDiscovery: true,
      );

      final discoveryMsg = fakeMqttClient.publishedMessages.firstWhere(
        (m) => m.topic == 'homeassistant/button/absorb_kids_tablet_sleep_chapter/config',
      );
      expect(discoveryMsg.retain, isTrue);

      final payload = jsonDecode(discoveryMsg.payload) as Map<String, dynamic>;
      expect(payload['unique_id'], equals('absorb_kids_tablet_sleep_chapter'));
      expect(payload['command_topic'], equals('absorb/kids_tablet/sleep_timer/set'));
      expect(jsonDecode(payload['payload_press']), equals({'mode': 'end_of_chapter'}));
      expect(payload['availability_topic'], equals('absorb/kids_tablet/status'));

      final device = payload['device'] as Map<String, dynamic>;
      expect(device['identifiers'], contains('absorb_kids_tablet'));
      expect(device['name'], equals('Absorb (kids_tablet)'));
    });

    test('dispatches STOP, NEXT, and PREVIOUS commands received on command topic', () async {
      when(() => mockAudioPlayerService.stop()).thenAnswer((_) async {});
      when(() => mockAudioPlayerService.skipToNextChapter()).thenAnswer((_) async {});
      when(() => mockAudioPlayerService.skipToPreviousChapter()).thenAnswer((_) async {});

      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'STOP');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.stop()).called(1);

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'NEXT');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.skipToNextChapter()).called(1);

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'PREVIOUS');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.skipToPreviousChapter()).called(1);
    });

    test('does not publish discovery when enableDiscovery is false', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
        enableDiscovery: false,
      );

      final discoveryTopics = fakeMqttClient.publishedMessages
          .map((m) => m.topic)
          .where((t) => t.startsWith('homeassistant/'));
      expect(discoveryTopics, isEmpty);
    });
  });

  group('MqttRemoteService Ticket #18: Disconnect Detection & Exponential Backoff Reconnect Loop', () {
    late MockAudioPlayerService mockAudioPlayerService;
    late FakeMqttClientAdapter fakeMqttClient;
    late MqttRemoteService service;

    setUp(() {
      SharedPreferences.setMockInitialValues({
        'mqtt_enabled': true,
        'mqtt_host': '192.168.1.50',
        'mqtt_port': 1883,
        'mqtt_slug': 'kids_tablet',
      });
      mockAudioPlayerService = MockAudioPlayerService();
      fakeMqttClient = FakeMqttClientAdapter();

      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
      when(() => mockAudioPlayerService.nowPlayingTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentAuthor).thenReturn('');
      when(() => mockAudioPlayerService.currentCoverUrl).thenReturn(null);
      when(() => mockAudioPlayerService.totalDuration).thenReturn(0.0);
      when(() => mockAudioPlayerService.position).thenReturn(Duration.zero);
      when(() => mockAudioPlayerService.volume).thenReturn(1.0);
      when(() => mockAudioPlayerService.speed).thenReturn(1.0);
      when(() => mockAudioPlayerService.chapters).thenReturn([]);
      when(() => mockAudioPlayerService.currentChapter).thenReturn(null);

      service = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayerService,
        clientAdapter: fakeMqttClient,
        enableJitter: false,
      );
    });

    tearDown(() {
      service.dispose();
      fakeMqttClient.dispose();
    });

    test('MqttClientAdapter exposes disconnect callback and hooks onDisconnected', () {
      bool callbackFired = false;
      fakeMqttClient.onDisconnected = () {
        callbackFired = true;
      };

      fakeMqttClient.simulateDisconnect();
      expect(callbackFired, isTrue);
    });

    test('unexpected disconnect transitions connection status to disconnected', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );
      expect(service.connectionStatus, equals(MqttConnectionStatus.connected));

      fakeMqttClient.simulateDisconnect();
      expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));
    });

    test('reconnect loop executes exponential backoff (2s, 4s, 8s, 16s, 32s, 60s max) when enableJitter is false', () {
      fakeAsync((async) {
        service.connect(
          host: '192.168.1.50',
          slug: 'kids_tablet',
        );
        async.flushMicrotasks();
        expect(service.connectionStatus, equals(MqttConnectionStatus.connected));

        // Trigger unexpected disconnect
        fakeMqttClient.simulateDisconnect();
        async.flushMicrotasks();

        expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));
        expect(service.isReconnecting, isTrue);
        expect(service.reconnectAttempts, equals(0));

        expect(service.computeReconnectDelay(0), equals(const Duration(seconds: 2)));
        expect(service.computeReconnectDelay(1), equals(const Duration(seconds: 4)));
        expect(service.computeReconnectDelay(2), equals(const Duration(seconds: 8)));
        expect(service.computeReconnectDelay(3), equals(const Duration(seconds: 16)));
        expect(service.computeReconnectDelay(4), equals(const Duration(seconds: 32)));
        expect(service.computeReconnectDelay(5), equals(const Duration(seconds: 60)));
        expect(service.computeReconnectDelay(6), equals(const Duration(seconds: 60)));

        // Advance 1s: still waiting on retry timer
        async.elapse(const Duration(seconds: 1));
        expect(service.isReconnecting, isTrue);

        // Advance 1 more second (2s total): 1st reconnect attempt fires
        async.elapse(const Duration(seconds: 1));
        async.flushMicrotasks();

        // Successful reconnect restores connected state and resets retry counters
        expect(service.connectionStatus, equals(MqttConnectionStatus.connected));
        expect(service.isReconnecting, isFalse);
        expect(service.reconnectAttempts, equals(0));
      });
    });

    test('reconnect loop increments attempts and schedules next backoff if reconnect fails', () {
      fakeAsync((async) {
        service.connect(
          host: '192.168.1.50',
          slug: 'kids_tablet',
        );
        async.flushMicrotasks();

        // Simulate unexpected disconnect
        fakeMqttClient.simulateDisconnect();
        async.flushMicrotasks();
        expect(service.reconnectAttempts, equals(0));

        // Make subsequent connection attempts fail
        fakeMqttClient.shouldSucceed = false;

        // 1st attempt at 2s: fails
        async.elapse(const Duration(seconds: 2));
        async.flushMicrotasks();
        expect(service.connectionStatus, equals(MqttConnectionStatus.error));
        expect(service.reconnectAttempts, equals(1));
        expect(service.isReconnecting, isTrue);

        // Advance 3s (not reached 4s backoff yet)
        async.elapse(const Duration(seconds: 3));
        expect(service.reconnectAttempts, equals(1));

        // Advance 1 more second (4s backoff reached): 2nd attempt fails
        async.elapse(const Duration(seconds: 1));
        async.flushMicrotasks();
        expect(service.reconnectAttempts, equals(2));
        expect(service.isReconnecting, isTrue);

        // Advance 8s backoff: 3rd attempt succeeds when shouldSucceed restored
        fakeMqttClient.shouldSucceed = true;
        async.elapse(const Duration(seconds: 8));
        async.flushMicrotasks();

        expect(service.connectionStatus, equals(MqttConnectionStatus.connected));
        expect(service.reconnectAttempts, equals(0));
        expect(service.isReconnecting, isFalse);
      });
    });

    test('jitter is applied within +/- 15% when enableJitter is true', () {
      final jitterService = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayerService,
        clientAdapter: fakeMqttClient,
        enableJitter: true,
        random: math.Random(42),
      );

      final d0 = jitterService.computeReconnectDelay(0);
      expect(d0.inMilliseconds, inInclusiveRange(1700, 2300));

      final d1 = jitterService.computeReconnectDelay(1);
      expect(d1.inMilliseconds, inInclusiveRange(3400, 4600));

      final d2 = jitterService.computeReconnectDelay(2);
      expect(d2.inMilliseconds, inInclusiveRange(6800, 9200));

      final d5 = jitterService.computeReconnectDelay(5);
      expect(d5.inMilliseconds, inInclusiveRange(51000, 60000));
    });

    test('explicit disconnect cancels active reconnection timer and resets retry backoff', () {
      fakeAsync((async) {
        service.connect(
          host: '192.168.1.50',
          slug: 'kids_tablet',
        );
        async.flushMicrotasks();

        fakeMqttClient.simulateDisconnect();
        async.flushMicrotasks();
        expect(service.isReconnecting, isTrue);

        // Explicit disconnect
        service.disconnect();
        expect(service.isReconnecting, isFalse);
        expect(service.reconnectAttempts, equals(0));

        // Advance 10s: no reconnection fires
        async.elapse(const Duration(seconds: 10));
        expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));
        expect(service.isReconnecting, isFalse);
      });
    });

    test('disabling MQTT cancels active reconnection timer when timer fires', () {
      fakeAsync((async) {
        service.connect(
          host: '192.168.1.50',
          slug: 'kids_tablet',
        );
        async.flushMicrotasks();

        fakeMqttClient.simulateDisconnect();
        async.flushMicrotasks();
        expect(service.isReconnecting, isTrue);

        // Disable MQTT in settings
        MqttSettings.setEnabled(false);

        // Advance 2s: timer fires, discovers disabled, disconnects cleanly
        async.elapse(const Duration(seconds: 2));
        async.flushMicrotasks();

        expect(service.isReconnecting, isFalse);
        expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));
      });
    });
  });

  group('MqttRemoteService Ticket #19: Network Recovery & Proactive Reconnect', () {
    late MockAudioPlayerService mockAudioPlayerService;
    late FakeMqttClientAdapter fakeMqttClient;
    late StreamController<List<ConnectivityResult>> connectivityController;
    late MqttRemoteService service;

    setUp(() async {
      SharedPreferences.setMockInitialValues({
        'mqtt_enabled': true,
        'mqtt_host': '192.168.1.50',
        'mqtt_port': 1883,
        'mqtt_slug': 'kids_tablet',
      });
      await SharedPreferences.getInstance();
      mockAudioPlayerService = MockAudioPlayerService();
      fakeMqttClient = FakeMqttClientAdapter();
      connectivityController = StreamController<List<ConnectivityResult>>.broadcast();

      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
      when(() => mockAudioPlayerService.nowPlayingTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentAuthor).thenReturn('');
      when(() => mockAudioPlayerService.currentCoverUrl).thenReturn(null);
      when(() => mockAudioPlayerService.totalDuration).thenReturn(0.0);
      when(() => mockAudioPlayerService.position).thenReturn(Duration.zero);
      when(() => mockAudioPlayerService.volume).thenReturn(1.0);
      when(() => mockAudioPlayerService.speed).thenReturn(1.0);
      when(() => mockAudioPlayerService.chapters).thenReturn([]);
      when(() => mockAudioPlayerService.currentChapter).thenReturn(null);

      service = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayerService,
        clientAdapter: fakeMqttClient,
        enableJitter: false,
        connectivityStream: connectivityController.stream,
      );
    });

    tearDown(() {
      service.dispose();
      fakeMqttClient.dispose();
      connectivityController.close();
    });

    test('accepts optional connectivity stream and tracks connectivity state', () async {
      await MqttSettings.setEnabled(false);
      expect(service.isNetworkOnline, isFalse);

      connectivityController.add([ConnectivityResult.wifi]);
      await Future<void>.delayed(Duration.zero);
      expect(service.isNetworkOnline, isTrue);

      connectivityController.add([ConnectivityResult.none]);
      await Future<void>.delayed(Duration.zero);
      expect(service.isNetworkOnline, isFalse);
    });

    test('triggers immediate reconnection when transitioning from offline to online if MQTT is enabled and currently disconnected', () async {
      // Initially disconnected
      expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));

      // Start with offline network
      connectivityController.add([ConnectivityResult.none]);
      await Future<void>.delayed(Duration.zero);
      expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));

      // Network restored to Wi-Fi
      connectivityController.add([ConnectivityResult.wifi]);
      await Future<void>.delayed(const Duration(milliseconds: 10));

      // Immediately connects without waiting for timer
      expect(service.connectionStatus, equals(MqttConnectionStatus.connected));
      expect(service.isReconnecting, isFalse);
      expect(service.reconnectAttempts, equals(0));
    });

    test('triggers immediate reconnection when transitioning from offline to online if in error state', () async {
      // Simulate failed connect attempt resulting in error
      fakeMqttClient.shouldSucceed = false;
      await service.connectFromSettings();
      expect(service.connectionStatus, equals(MqttConnectionStatus.error));

      // Network transitions to offline then restored to mobile
      connectivityController.add([ConnectivityResult.none]);
      await Future<void>.delayed(Duration.zero);

      fakeMqttClient.shouldSucceed = true;
      connectivityController.add([ConnectivityResult.mobile]);
      await Future<void>.delayed(const Duration(milliseconds: 10));

      expect(service.connectionStatus, equals(MqttConnectionStatus.connected));
      expect(service.reconnectAttempts, equals(0));
    });

    test('does not trigger reconnection on network restored if MQTT is disabled in settings', () async {
      await MqttSettings.setEnabled(false);

      connectivityController.add([ConnectivityResult.none]);
      await Future<void>.delayed(Duration.zero);

      connectivityController.add([ConnectivityResult.wifi]);
      await Future<void>.delayed(const Duration(milliseconds: 10));

      expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));
      expect(service.isReconnecting, isFalse);
    });

    test('does not trigger reconnection on network restored if already connected', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );
      expect(service.connectionStatus, equals(MqttConnectionStatus.connected));

      // Network stream emits wifi while already connected
      connectivityController.add([ConnectivityResult.wifi]);
      await Future<void>.delayed(const Duration(milliseconds: 10));

      // Remains connected, does not re-enter connect flow
      expect(service.connectionStatus, equals(MqttConnectionStatus.connected));
    });

    test('cancels active backoff retry timer and resets backoff retry delays upon successful reconnect on network restored', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );
      expect(service.connectionStatus, equals(MqttConnectionStatus.connected));

      // Disconnect unexpectedly
      fakeMqttClient.simulateDisconnect();
      await Future<void>.delayed(Duration.zero);
      expect(service.isReconnecting, isTrue);
      expect(service.reconnectAttempts, equals(0));

      // Network drops
      connectivityController.add([ConnectivityResult.none]);
      await Future<void>.delayed(Duration.zero);
      expect(service.isReconnecting, isTrue);

      // Network restores now before timer expires!
      connectivityController.add([ConnectivityResult.wifi]);
      await Future<void>.delayed(const Duration(milliseconds: 10));

      // Reconnect fired immediately, timer cancelled, attempts reset
      expect(service.connectionStatus, equals(MqttConnectionStatus.connected));
      expect(service.isReconnecting, isFalse);
      expect(service.reconnectAttempts, equals(0));
    });

    test('resets retry delay and reschedules backoff if reconnect attempt fails upon network restoration', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      // Simulate unexpected disconnect
      fakeMqttClient.simulateDisconnect();
      await Future<void>.delayed(Duration.zero);
      expect(service.isReconnecting, isTrue);

      // Network drops then comes online, but broker is unreachable
      fakeMqttClient.shouldSucceed = false;
      connectivityController.add([ConnectivityResult.none]);
      await Future<void>.delayed(Duration.zero);

      connectivityController.add([ConnectivityResult.ethernet]);
      await Future<void>.delayed(const Duration(milliseconds: 10));

      // Reconnect attempted immediately and failed: attempts reset to 0 then incremented to 1
      expect(service.connectionStatus, equals(MqttConnectionStatus.error));
      expect(service.reconnectAttempts, equals(1));
      expect(service.isReconnecting, isTrue);
      // Next backoff is for attempt 1 (4s), NOT 32s
      expect(service.computeReconnectDelay(service.reconnectAttempts), equals(const Duration(seconds: 4)));
    });

    test('does not trigger reconnection on redundant online transitions without prior offline state', () async {
      connectivityController.add([ConnectivityResult.wifi]);
      await Future<void>.delayed(const Duration(milliseconds: 10));
      expect(service.connectionStatus, equals(MqttConnectionStatus.connected));

      // Disconnect explicitly
      service.disconnect();
      expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));

      // Emitting another online result (e.g. cellular or vpn) without offline first
      connectivityController.add([ConnectivityResult.mobile]);
      await Future<void>.delayed(const Duration(milliseconds: 10));

      // Should NOT have triggered reconnect
      expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));
    });

    test('connectivity subscription persists across disconnect and reconnect cycles until disposed', () async {
      connectivityController.add([ConnectivityResult.wifi]);
      await Future<void>.delayed(const Duration(milliseconds: 10));
      expect(service.connectionStatus, equals(MqttConnectionStatus.connected));

      // Disconnect
      service.disconnect();
      expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));

      // Cycle network: offline -> online
      connectivityController.add([ConnectivityResult.none]);
      await Future<void>.delayed(Duration.zero);
      connectivityController.add([ConnectivityResult.wifi]);
      await Future<void>.delayed(const Duration(milliseconds: 10));

      // Reconnected!
      expect(service.connectionStatus, equals(MqttConnectionStatus.connected));

      // Dispose cancels connectivity subscription
      service.dispose();
      connectivityController.add([ConnectivityResult.none]);
      await Future<void>.delayed(Duration.zero);
      connectivityController.add([ConnectivityResult.wifi]);
      await Future<void>.delayed(const Duration(milliseconds: 10));

      expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));
    });
  });

  group('MqttRemoteService Ticket #20: Transport Gap Closure & Playback Speed', () {
    late MockAudioPlayerService mockAudioPlayerService;
    late FakeMqttClientAdapter fakeMqttClient;
    late MqttRemoteService service;

    setUp(() async {
      SharedPreferences.setMockInitialValues({
        'mqtt_enabled': true,
        'mqtt_host': '192.168.1.50',
        'mqtt_port': 1883,
        'mqtt_slug': 'kids_tablet',
      });
      await SharedPreferences.getInstance();
      mockAudioPlayerService = MockAudioPlayerService();
      fakeMqttClient = FakeMqttClientAdapter();

      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
      when(() => mockAudioPlayerService.nowPlayingTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentAuthor).thenReturn('');
      when(() => mockAudioPlayerService.currentCoverUrl).thenReturn(null);
      when(() => mockAudioPlayerService.totalDuration).thenReturn(0.0);
      when(() => mockAudioPlayerService.position).thenReturn(Duration.zero);
      when(() => mockAudioPlayerService.volume).thenReturn(1.0);
      when(() => mockAudioPlayerService.speed).thenReturn(1.0);
      when(() => mockAudioPlayerService.chapters).thenReturn([]);
      when(() => mockAudioPlayerService.currentChapter).thenReturn(null);
      when(() => mockAudioPlayerService.play(fromUi: any(named: 'fromUi'))).thenAnswer((_) async {});
      when(() => mockAudioPlayerService.pause()).thenAnswer((_) async {});
      when(() => mockAudioPlayerService.setSpeed(any())).thenAnswer((_) async {});

      service = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayerService,
        clientAdapter: fakeMqttClient,
        enableJitter: false,
      );
    });

    tearDown(() {
      service.dispose();
      fakeMqttClient.dispose();
    });

    test('subscribes to absorb/<slug>/speed/set upon connection', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      expect(fakeMqttClient.subscriptions, contains('absorb/kids_tablet/speed/set'));
    });

    test('inbound numeric payload on speed topic updates AudioPlayerService.setSpeed()', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/speed/set', '1.25');
      await Future<void>.delayed(Duration.zero);

      verify(() => mockAudioPlayerService.setSpeed(1.25)).called(1);
    });

    test('clamps speed values between 0.5 and 3.0', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      // Below 0.5 clamped to 0.5
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/speed/set', '0.2');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.setSpeed(0.5)).called(1);

      // Negative value clamped to 0.5
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/speed/set', '-1.0');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.setSpeed(0.5)).called(1);

      // Above 3.0 clamped to 3.0
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/speed/set', '3.5');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.setSpeed(3.0)).called(1);

      // 5.0 clamped to 3.0
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/speed/set', '5.0');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockAudioPlayerService.setSpeed(3.0)).called(1);
    });

    test('gracefully ignores invalid non-numeric speed payloads', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/speed/set', 'invalid');
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/speed/set', '');
      await Future<void>.delayed(Duration.zero);

      verifyNever(() => mockAudioPlayerService.setSpeed(any()));
    });

    test('PLAY_PAUSE command pauses when player is currently playing', () async {
      when(() => mockAudioPlayerService.isPlaying).thenReturn(true);

      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'PLAY_PAUSE');
      await Future<void>.delayed(Duration.zero);

      verify(() => mockAudioPlayerService.pause()).called(1);
      verifyNever(() => mockAudioPlayerService.play(fromUi: any(named: 'fromUi')));
    });

    test('PLAY_PAUSE command plays when player is currently paused', () async {
      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);

      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'PLAY_PAUSE');
      await Future<void>.delayed(Duration.zero);

      verify(() => mockAudioPlayerService.play(fromUi: false)).called(1);
      verifyNever(() => mockAudioPlayerService.pause());
    });

    test('state telemetry publish reflects updated speed', () async {
      when(() => mockAudioPlayerService.speed).thenReturn(1.5);

      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.publishedMessages.clear();

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/speed/set', '1.5');
      await Future<void>.delayed(Duration.zero);

      final stateMessages = fakeMqttClient.publishedMessages
          .where((m) => m.topic == 'absorb/kids_tablet/state')
          .toList();
      expect(stateMessages, isNotEmpty);
      final latestState = jsonDecode(stateMessages.last.payload) as Map<String, dynamic>;
      expect(latestState['speed'], equals(1.5));
    });

    test('state telemetry publish reflects updated playback state on PLAY_PAUSE', () async {
      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);

      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      when(() => mockAudioPlayerService.isPlaying).thenReturn(true);
      fakeMqttClient.publishedMessages.clear();

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'PLAY_PAUSE');
      await Future<void>.delayed(Duration.zero);

      final stateMessages = fakeMqttClient.publishedMessages
          .where((m) => m.topic == 'absorb/kids_tablet/state')
          .toList();
      expect(stateMessages, isNotEmpty);
      final latestState = jsonDecode(stateMessages.last.payload) as Map<String, dynamic>;
      expect(latestState['state'], equals('playing'));
    });
  });

  group('MqttRemoteService Ticket #21: Permissive Sleep Timer Command Parsing', () {
    late MockAudioPlayerService mockAudioPlayerService;
    late MockSleepTimerService mockSleepTimerService;
    late FakeMqttClientAdapter fakeMqttClient;
    late MqttRemoteService service;

    setUp(() async {
      SharedPreferences.setMockInitialValues({
        'mqtt_enabled': true,
        'mqtt_host': '192.168.1.50',
        'mqtt_port': 1883,
        'mqtt_slug': 'kids_tablet',
      });
      await SharedPreferences.getInstance();
      mockAudioPlayerService = MockAudioPlayerService();
      mockSleepTimerService = MockSleepTimerService();
      fakeMqttClient = FakeMqttClientAdapter();

      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
      when(() => mockAudioPlayerService.nowPlayingTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentAuthor).thenReturn('');
      when(() => mockAudioPlayerService.currentCoverUrl).thenReturn(null);
      when(() => mockAudioPlayerService.totalDuration).thenReturn(0.0);
      when(() => mockAudioPlayerService.position).thenReturn(Duration.zero);
      when(() => mockAudioPlayerService.volume).thenReturn(1.0);
      when(() => mockAudioPlayerService.speed).thenReturn(1.0);
      when(() => mockAudioPlayerService.chapters).thenReturn([]);
      when(() => mockAudioPlayerService.currentChapter).thenReturn(null);

      when(() => mockSleepTimerService.isActive).thenReturn(false);
      when(() => mockSleepTimerService.mode).thenReturn(SleepTimerMode.off);
      when(() => mockSleepTimerService.timeRemaining).thenReturn(Duration.zero);
      when(() => mockSleepTimerService.initialDuration).thenReturn(Duration.zero);

      service = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayerService,
        sleepTimerService: mockSleepTimerService,
        clientAdapter: fakeMqttClient,
        enableJitter: false,
      );
    });

    tearDown(() {
      service.dispose();
      fakeMqttClient.dispose();
    });

    test('inbound payload "cancel" cancels the active sleep timer', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        'cancel',
      );
      await Future<void>.delayed(Duration.zero);

      verify(() => mockSleepTimerService.cancel()).called(1);

      // Case insensitive
      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        'CANCEL',
      );
      await Future<void>.delayed(Duration.zero);

      verify(() => mockSleepTimerService.cancel()).called(1);
    });

    test('inbound payload "end_of_chapter" or "chapter" sets end-of-chapter sleep timer', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        'end_of_chapter',
      );
      await Future<void>.delayed(Duration.zero);

      verify(() => mockSleepTimerService.setChapterSleep(1)).called(1);

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        'chapter',
      );
      await Future<void>.delayed(Duration.zero);

      verify(() => mockSleepTimerService.setChapterSleep(1)).called(1);

      // Uppercase variant
      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        'END_OF_CHAPTER',
      );
      await Future<void>.delayed(Duration.zero);

      verify(() => mockSleepTimerService.setChapterSleep(1)).called(1);
    });

    test('inbound raw integer or numeric string sets timed sleep timer for that duration in minutes', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        '15',
      );
      await Future<void>.delayed(Duration.zero);

      verify(() => mockSleepTimerService.setTimeSleep(const Duration(minutes: 15))).called(1);

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        '45',
      );
      await Future<void>.delayed(Duration.zero);

      verify(() => mockSleepTimerService.setTimeSleep(const Duration(minutes: 45))).called(1);
    });

    test('existing JSON payloads continue to function without regression', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        jsonEncode({'duration_minutes': 30}),
      );
      await Future<void>.delayed(Duration.zero);
      verify(() => mockSleepTimerService.setTimeSleep(const Duration(minutes: 30))).called(1);

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        jsonEncode({'mode': 'end_of_chapter'}),
      );
      await Future<void>.delayed(Duration.zero);
      verify(() => mockSleepTimerService.setChapterSleep(1)).called(1);

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/set',
        jsonEncode({'cancel': true}),
      );
      await Future<void>.delayed(Duration.zero);
      verify(() => mockSleepTimerService.cancel()).called(1);
    });

    test('inbound quoted string commands and numbers are parsed correctly', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/sleep_timer/set', '"cancel"');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockSleepTimerService.cancel()).called(1);

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/sleep_timer/set', '"end_of_chapter"');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockSleepTimerService.setChapterSleep(1)).called(1);

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/sleep_timer/set', '"30"');
      await Future<void>.delayed(Duration.zero);
      verify(() => mockSleepTimerService.setTimeSleep(const Duration(minutes: 30))).called(1);
    });

    test('gracefully ignores invalid, empty, or non-positive sleep timer payloads', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/sleep_timer/set', '');
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/sleep_timer/set', '   ');
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/sleep_timer/set', 'invalid_mode');
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/sleep_timer/set', '-10');
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/sleep_timer/set', '0');
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/sleep_timer/set', 'Infinity');
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/sleep_timer/set', '{malformed json');
      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/sleep_timer/set', jsonEncode({'cancel': false}));
      await Future<void>.delayed(Duration.zero);

      verifyNever(() => mockSleepTimerService.setTimeSleep(any()));
      verifyNever(() => mockSleepTimerService.setChapterSleep(any()));
      verifyNever(() => mockSleepTimerService.cancel());
    });
  });

  group('MqttRemoteService Ticket #22: Home Assistant Sleep Timer Number Entity Discovery & Duration Slider', () {
    late MockAudioPlayerService mockAudioPlayerService;
    late MockSleepTimerService mockSleepTimerService;
    late FakeMqttClientAdapter fakeMqttClient;
    late MqttRemoteService service;

    setUp(() async {
      SharedPreferences.setMockInitialValues({
        'mqtt_enabled': true,
        'mqtt_host': '192.168.1.50',
        'mqtt_port': 1883,
        'mqtt_slug': 'kids_tablet',
      });
      await SharedPreferences.getInstance();
      mockAudioPlayerService = MockAudioPlayerService();
      mockSleepTimerService = MockSleepTimerService();
      fakeMqttClient = FakeMqttClientAdapter();

      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
      when(() => mockAudioPlayerService.nowPlayingTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentTitle).thenReturn('');
      when(() => mockAudioPlayerService.currentAuthor).thenReturn('');
      when(() => mockAudioPlayerService.currentCoverUrl).thenReturn(null);
      when(() => mockAudioPlayerService.totalDuration).thenReturn(0.0);
      when(() => mockAudioPlayerService.position).thenReturn(Duration.zero);
      when(() => mockAudioPlayerService.volume).thenReturn(1.0);
      when(() => mockAudioPlayerService.speed).thenReturn(1.0);
      when(() => mockAudioPlayerService.chapters).thenReturn([]);
      when(() => mockAudioPlayerService.currentChapter).thenReturn(null);

      when(() => mockSleepTimerService.isActive).thenReturn(false);
      when(() => mockSleepTimerService.mode).thenReturn(SleepTimerMode.off);
      when(() => mockSleepTimerService.timeRemaining).thenReturn(Duration.zero);
      when(() => mockSleepTimerService.initialDuration).thenReturn(Duration.zero);

      service = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayerService,
        sleepTimerService: mockSleepTimerService,
        clientAdapter: fakeMqttClient,
        enableJitter: false,
      );
    });

    tearDown(() {
      service.dispose();
      fakeMqttClient.dispose();
    });

    test('publishes retained sleep timer number entity discovery configuration on connect', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
        enableDiscovery: true,
      );

      final discoveryMsg = fakeMqttClient.publishedMessages.firstWhere(
        (m) => m.topic == 'homeassistant/number/absorb_kids_tablet_sleep_timer/config',
      );
      expect(discoveryMsg.retain, isTrue);

      final payload = jsonDecode(discoveryMsg.payload) as Map<String, dynamic>;
      expect(payload['name'], equals('Absorb (kids_tablet) Sleep Timer Duration'));
      expect(payload['unique_id'], equals('absorb_kids_tablet_sleep_timer'));
      expect(payload['command_topic'], equals('absorb/kids_tablet/sleep_timer/duration/set'));
      expect(payload['min'], equals(0));
      expect(payload['max'], equals(120));
      expect(payload['step'], equals(5));
      expect(payload['unit_of_measurement'], equals('min'));
      expect(payload['unit'], equals('min'));
      expect(payload['icon'], equals('mdi:timer-outline'));
      expect(payload['availability_topic'], equals('absorb/kids_tablet/status'));
      expect(payload['payload_available'], equals('online'));
      expect(payload['payload_not_available'], equals('offline'));

      final device = payload['device'] as Map<String, dynamic>;
      expect(device['identifiers'], contains('absorb_kids_tablet'));
      expect(device['name'], equals('Absorb (kids_tablet)'));
    });

    test('subscribes to absorb/<slug>/sleep_timer/duration/set upon connection', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      expect(
        fakeMqttClient.subscriptions,
        contains('absorb/kids_tablet/sleep_timer/duration/set'),
      );
    });

    test('inbound positive duration payload sets sleep timer for that duration', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/duration/set',
        '30',
      );
      await Future<void>.delayed(Duration.zero);
      verify(() => mockSleepTimerService.setTimeSleep(const Duration(minutes: 30))).called(1);

      // Float representation
      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/duration/set',
        '45.0',
      );
      await Future<void>.delayed(Duration.zero);
      verify(() => mockSleepTimerService.setTimeSleep(const Duration(minutes: 45))).called(1);

      // Clamps values above 120
      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/duration/set',
        '150',
      );
      await Future<void>.delayed(Duration.zero);
      verify(() => mockSleepTimerService.setTimeSleep(const Duration(minutes: 120))).called(1);
    });

    test('inbound 0 duration payload cancels active sleep timer while negative durations are ignored', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/duration/set',
        '0',
      );
      await Future<void>.delayed(Duration.zero);
      verify(() => mockSleepTimerService.cancel()).called(1);

      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/duration/set',
        '0.0',
      );
      await Future<void>.delayed(Duration.zero);
      verify(() => mockSleepTimerService.cancel()).called(1);

      // Negative numbers should not cancel or trigger sleep timer
      fakeMqttClient.simulateInboundMessage(
        'absorb/kids_tablet/sleep_timer/duration/set',
        '-10',
      );
      await Future<void>.delayed(Duration.zero);
      verifyNever(() => mockSleepTimerService.cancel());
      verifyNever(() => mockSleepTimerService.setTimeSleep(any()));
    });

    test('unpublishDiscovery emits empty retained discovery messages across discovery topics', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
        enableDiscovery: true,
      );

      service.unpublishDiscovery();

      final unpublishedNumber = fakeMqttClient.publishedMessages.firstWhere(
        (m) => m.topic == 'homeassistant/number/absorb_kids_tablet_sleep_timer/config' && m.payload.isEmpty,
      );
      expect(unpublishedNumber.retain, isTrue);
    });

    test('disconnect emits offline availability status retained', () async {
      await service.connect(
        host: '192.168.1.50',
        slug: 'kids_tablet',
      );

      service.disconnect();

      final offlineMsg = fakeMqttClient.publishedMessages.firstWhere(
        (m) => m.topic == 'absorb/kids_tablet/status' && m.payload == 'offline',
      );
      expect(offlineMsg.retain, isTrue);
    });
  });
}
