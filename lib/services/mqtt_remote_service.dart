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

class MqttConnectionConfig {
  final String host;
  final int port;
  final String identifier;
  final String willTopic;
  final String willMessage;
  final bool willRetain;
  final bool cleanSession;
  final String? username;
  final String? password;
  final bool useTls;

  const MqttConnectionConfig({
    required this.host,
    required this.port,
    required this.identifier,
    required this.willTopic,
    required this.willMessage,
    this.willRetain = true,
    this.cleanSession = true,
    this.username,
    this.password,
    this.useTls = false,
  });
}

abstract class MqttClientAdapter {
  Future<bool> connect(MqttConnectionConfig config);
  void disconnect();
  void subscribe(String topic);
  void publish(String topic, String payload, {bool retain = false});
  Stream<({String topic, String payload})> get incomingMessages;
  bool get isConnected;
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
  Future<bool> connect(MqttConnectionConfig config) async {
    final client = MqttServerClient.withPort(
      config.host,
      config.identifier,
      config.port,
    );
    client.secure = config.useTls;
    client.keepAlivePeriod = 20;
    client.logging(on: false);

    var connMess = MqttConnectMessage()
        .withClientIdentifier(config.identifier)
        .withWillTopic(config.willTopic)
        .withWillMessage(config.willMessage)
        .withWillQos(MqttQos.atLeastOnce);

    if (config.willRetain) {
      connMess = connMess.withWillRetain();
    }
    if (config.cleanSession) {
      connMess = connMess.startClean();
    }
    final user = config.username;
    if (user != null && user.isNotEmpty) {
      connMess = connMess.authenticateAs(user, config.password ?? '');
    }

    client.connectionMessage = connMess;

    try {
      final status = await client.connect(config.username, config.password);
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
  String get statusTopic => 'absorb/$_slug/status';
  String get commandTopic => 'absorb/$_slug/set';
  String get stateTopic => 'absorb/$_slug/state';

  static String sanitizeSlug(String rawSlug) {
    final sanitized = rawSlug.trim().replaceAll(RegExp(r'[^a-zA-Z0-9_-]'), '_');
    return sanitized.isEmpty ? 'absorb' : sanitized.toLowerCase();
  }

  Future<bool> connect({
    required String host,
    int? port,
    required String slug,
    String? username,
    String? password,
    bool useTls = false,
  }) async {
    _slug = sanitizeSlug(slug);
    final resolvedPort = port ?? (useTls ? 8883 : 1883);
    final clientIdentifier =
        'absorb_${_slug}_${DateTime.now().millisecondsSinceEpoch % 100000}';

    final config = MqttConnectionConfig(
      host: host,
      port: resolvedPort,
      identifier: clientIdentifier,
      willTopic: statusTopic,
      willMessage: 'offline',
      willRetain: true,
      cleanSession: true,
      username: username,
      password: password,
      useTls: useTls,
    );

    final success = await _clientAdapter.connect(config);
    if (!success) {
      return false;
    }

    // Publish online status retained
    _clientAdapter.publish(statusTopic, 'online', retain: true);

    // Subscribe to command topic
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
    if (topic != commandTopic) return;

    final command = payload.trim().toUpperCase();
    if (command == 'PLAY') {
      _audioPlayerService.play(fromUi: false);
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
        stateTopic,
        currentState,
        retain: false,
      );
    }
  }

  void disconnect() {
    if (_clientAdapter.isConnected) {
      _clientAdapter.publish(statusTopic, 'offline', retain: true);
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
