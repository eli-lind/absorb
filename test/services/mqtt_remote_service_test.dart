import 'dart:async';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
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

  group('MqttRemoteService Tracer Bullet', () {
    late MockAudioPlayerService mockAudioPlayerService;
    late FakeMqttClientAdapter fakeMqttClient;
    late MqttRemoteService service;

    setUp(() {
      mockAudioPlayerService = MockAudioPlayerService();
      fakeMqttClient = FakeMqttClientAdapter();
      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
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
      expect(stateMsg.payload, equals('playing'));
      expect(stateMsg.retain, isFalse);

      // Transition to paused
      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
      service.onPlayerStateChanged();

      stateMsg = fakeMqttClient.publishedMessages.lastWhere(
        (m) => m.topic == 'absorb/kids_tablet/state',
      );
      expect(stateMsg.payload, equals('paused'));
      expect(stateMsg.retain, isFalse);
    });
  });
}
