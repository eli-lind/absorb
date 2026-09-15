import 'dart:async';
import 'dart:convert';
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



class MockAudioPlayerService extends Mock implements AudioPlayerService {}

class MockSleepTimerService extends Mock implements SleepTimerService {}

class MockApiService extends Mock implements ApiService {}

class MockDownloadService extends Mock implements DownloadService {}

class FakeMqttClientAdapter implements MqttClientAdapter {
  MqttConnectionConfig? lastConfig;
  bool _isConnected = false;

  final List<String> subscriptions = [];
  final List<PublishedMqttMessage> publishedMessages = [];
  final StreamController<({String topic, String payload})> _incomingController =
      StreamController<({String topic, String payload})>.broadcast();

  @override
  Stream<({String topic, String payload})> get incomingMessages =>
      _incomingController.stream;

  @override
  bool get isConnected => _isConnected;

  @override
  Future<bool> connect(MqttConnectionConfig config) async {
    lastConfig = config;
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
}
