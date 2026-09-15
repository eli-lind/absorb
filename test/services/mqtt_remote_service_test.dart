import 'dart:async';
import 'dart:convert';
import 'package:fake_async/fake_async.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:absorb/services/mqtt_remote_service.dart';
import 'package:absorb/services/audio_player_service.dart';



class MockAudioPlayerService extends Mock implements AudioPlayerService {}

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

}
