import 'dart:async';
import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:mqtt_client/mqtt_client.dart';
import 'package:mqtt_client/mqtt_server_client.dart';
import 'audio_player_service.dart';

class PublishedMqttMessage {
  final String topic;
  final String payload;
  final bool retain;

  PublishedMqttMessage({
    required this.topic,
    required this.payload,
    required this.retain,
  });
}

abstract class MqttClientAdapter {
  Future<bool> connect({
    required String host,
    required int port,
    required String identifier,
    required String willTopic,
    required String willMessage,
    required bool willRetain,
    required bool cleanSession,
    String? username,
    String? password,
    bool useTls,
  });

  void disconnect();
  void subscribe(String topic);
  void publish(String topic, String payload, {bool retain = false});
  Stream<({String topic, String payload})> get incomingMessages;
  bool get isConnected;
}

class FakeMqttClientAdapter implements MqttClientAdapter {
  String? lastHost;
  int? lastPort;
  String? lastIdentifier;
  String? willTopic;
  String? willMessage;
  bool willRetain = false;
  bool cleanSession = false;
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
  Future<bool> connect({
    required String host,
    required int port,
    required String identifier,
    required String willTopic,
    required String willMessage,
    required bool willRetain,
    required bool cleanSession,
    String? username,
    String? password,
    bool useTls = false,
  }) async {
    lastHost = host;
    lastPort = port;
    lastIdentifier = identifier;
    this.willTopic = willTopic;
    this.willMessage = willMessage;
    this.willRetain = willRetain;
    this.cleanSession = cleanSession;
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

class DefaultMqttClientAdapter implements MqttClientAdapter {
  MqttServerClient? _client;
  final StreamController<({String topic, String payload})> _incomingController =
      StreamController<({String topic, String payload})>.broadcast();
  StreamSubscription? _updateSub;

  @override
  Stream<({String topic, String payload})> get incomingMessages =>
      _incomingController.stream;

  @override
  bool get isConnected =>
      _client?.connectionStatus?.state == MqttConnectionState.connected;

  @override
  Future<bool> connect({
    required String host,
    required int port,
    required String identifier,
    required String willTopic,
    required String willMessage,
    required bool willRetain,
    required bool cleanSession,
    String? username,
    String? password,
    bool useTls = false,
  }) async {
    final client = MqttServerClient.withPort(host, identifier, port);
    client.secure = useTls;
    client.keepAlivePeriod = 20;
    client.logging(on: false);

    var connMess = MqttConnectMessage()
        .withClientIdentifier(identifier)
        .withWillTopic(willTopic)
        .withWillMessage(willMessage)
        .withWillQos(MqttQos.atLeastOnce);

    if (willRetain) {
      connMess = connMess.withWillRetain();
    }
    if (cleanSession) {
      connMess = connMess.startClean();
    }
    if (username != null && username.isNotEmpty) {
      connMess = connMess.authenticateAs(username, password ?? '');
    }

    client.connectionMessage = connMess;

    try {
      final status = await client.connect(username, password);
      if (status?.state == MqttConnectionState.connected) {
        _client = client;
        _updateSub = client.updates?.listen((messages) {
          for (final msg in messages) {
            final recMess = msg.payload as MqttPublishMessage;
            final payload = utf8.decode(recMess.payload.message);
            _incomingController.add((topic: msg.topic, payload: payload));
          }
        });
        return true;
      }
    } catch (e) {
      debugPrint('[MqttRemoteService] Connection error: $e');
      client.disconnect();
    }
    return false;
  }

  @override
  void disconnect() {
    _updateSub?.cancel();
    _client?.disconnect();
    _client = null;
  }

  @override
  void subscribe(String topic) {
    _client?.subscribe(topic, MqttQos.atLeastOnce);
  }

  @override
  void publish(String topic, String payload, {bool retain = false}) {
    final builder = MqttClientPayloadBuilder();
    builder.addString(payload);
    final data = builder.payload;
    if (data != null) {
      _client?.publishMessage(
        topic,
        MqttQos.atLeastOnce,
        data,
        retain: retain,
      );
    }
  }
}

class MqttRemoteService {
  static final MqttRemoteService _instance = MqttRemoteService._();
  factory MqttRemoteService() => _instance;
  MqttRemoteService._()
      : _audioPlayerService = AudioPlayerService(),
        _clientAdapter = DefaultMqttClientAdapter();

  @visibleForTesting
  MqttRemoteService.forTesting({
    required AudioPlayerService audioPlayerService,
    required MqttClientAdapter clientAdapter,
  })  : _audioPlayerService = audioPlayerService,
        _clientAdapter = clientAdapter;

  final AudioPlayerService _audioPlayerService;
  final MqttClientAdapter _clientAdapter;

  String _slug = 'absorb';
  StreamSubscription? _incomingSub;
  String? _lastReportedPlaybackState;
  bool _isPlayerListenerAttached = false;

  bool get isConnected => _clientAdapter.isConnected;
  String get slug => _slug;

  Future<bool> connect({
    required String host,
    int port = 1883,
    required String slug,
    String? username,
    String? password,
    bool useTls = false,
  }) async {
    _slug = slug.trim().isEmpty ? 'absorb' : slug.trim();
    final clientIdentifier = 'absorb_${_slug}_${DateTime.now().millisecondsSinceEpoch % 100000}';
    final statusTopic = 'absorb/$_slug/status';

    final success = await _clientAdapter.connect(
      host: host,
      port: port,
      identifier: clientIdentifier,
      willTopic: statusTopic,
      willMessage: 'offline',
      willRetain: true,
      cleanSession: true,
      username: username,
      password: password,
      useTls: useTls,
    );

    if (!success) {
      return false;
    }

    // Publish online status retained
    _clientAdapter.publish(statusTopic, 'online', retain: true);

    // Subscribe to command topic
    final commandTopic = 'absorb/$_slug/set';
    _clientAdapter.subscribe(commandTopic);

    // Listen to inbound commands
    await _incomingSub?.cancel();
    _incomingSub = _clientAdapter.incomingMessages.listen((msg) {
      _handleInboundMessage(msg.topic, msg.payload);
    });

    // Attach listener for playback state changes
    if (!_isPlayerListenerAttached) {
      _audioPlayerService.addListener(onPlayerStateChanged);
      _isPlayerListenerAttached = true;
    }

    // Initial state publish
    onPlayerStateChanged();

    return true;
  }

  void _handleInboundMessage(String topic, String payload) {
    final commandTopic = 'absorb/$_slug/set';
    if (topic != commandTopic) return;

    final command = payload.trim().toUpperCase();
    if (command == 'PLAY') {
      _audioPlayerService.play(fromUi: true);
    } else if (command == 'PAUSE') {
      _audioPlayerService.pause();
    }
  }

  void onPlayerStateChanged() {
    final isPlaying = _audioPlayerService.isPlaying;
    final currentState = isPlaying ? 'playing' : 'paused';

    if (currentState != _lastReportedPlaybackState) {
      _lastReportedPlaybackState = currentState;
      _clientAdapter.publish(
        'absorb/$_slug/state',
        currentState,
        retain: false,
      );
    }
  }

  void disconnect() {
    if (_clientAdapter.isConnected) {
      _clientAdapter.publish('absorb/$_slug/status', 'offline', retain: true);
    }
    _incomingSub?.cancel();
    _incomingSub = null;
    if (_isPlayerListenerAttached) {
      _audioPlayerService.removeListener(onPlayerStateChanged);
      _isPlayerListenerAttached = false;
    }
    _clientAdapter.disconnect();
    _lastReportedPlaybackState = null;
  }

  void dispose() {
    disconnect();
  }
}
