import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:absorb/services/mqtt_remote_service.dart';
import 'package:absorb/services/audio_player_service.dart';

class MockAudioPlayerService extends Mock implements AudioPlayerService {}

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
    });

    test('configures clean session and LWT to publish offline retained to absorb/<slug>/status', () async {
      await service.connect(
        host: '192.168.1.50',
        port: 1883,
        slug: 'kids_tablet',
      );

      expect(fakeMqttClient.lastHost, equals('192.168.1.50'));
      expect(fakeMqttClient.lastPort, equals(1883));
      expect(fakeMqttClient.cleanSession, isTrue);
      expect(fakeMqttClient.willTopic, equals('absorb/kids_tablet/status'));
      expect(fakeMqttClient.willMessage, equals('offline'));
      expect(fakeMqttClient.willRetain, isTrue);
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

    test('dispatches PLAY command to AudioPlayerService when received on absorb/<slug>/set', () async {
      await service.connect(
        host: '192.168.1.50',
        port: 1883,
        slug: 'kids_tablet',
      );

      fakeMqttClient.simulateInboundMessage('absorb/kids_tablet/set', 'PLAY');
      await Future<void>.delayed(Duration.zero);

      verify(() => mockAudioPlayerService.play(logDetail: any(named: 'logDetail'), fromUi: any(named: 'fromUi'))).called(1);
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

      // Transition to paused
      when(() => mockAudioPlayerService.isPlaying).thenReturn(false);
      service.onPlayerStateChanged();

      stateMsg = fakeMqttClient.publishedMessages.lastWhere(
        (m) => m.topic == 'absorb/kids_tablet/state',
      );
      expect(stateMsg.payload, equals('paused'));
    });
  });
}
