import 'dart:async';
import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:mqtt_client/mqtt_client.dart';
import 'package:mqtt_client/mqtt_server_client.dart';
import 'audio_player_service.dart';
import 'sleep_timer_service.dart';


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

typedef _PlayerSnapshot = ({
  bool isPlaying,
  String? book,
  String? chapter,
  double speed,
  double positionSec,
});

typedef _SleepTimerSnapshot = ({
  bool active,
  String mode,
  int remainingSeconds,
  int initialMinutes,
});

class MqttRemoteService {
  static final MqttRemoteService _instance = MqttRemoteService._();
  factory MqttRemoteService() => _instance;
  MqttRemoteService._()
      : _audioPlayerService = AudioPlayerService(),
        _sleepTimerService = SleepTimerService(),
        _clientAdapter = DefaultMqttClientAdapter();

  @visibleForTesting
  MqttRemoteService.forTesting({
    required AudioPlayerService audioPlayerService,
    SleepTimerService? sleepTimerService,
    required MqttClientAdapter clientAdapter,
  })  : _audioPlayerService = audioPlayerService,
        _sleepTimerService = sleepTimerService ?? SleepTimerService(),
        _clientAdapter = clientAdapter;

  final AudioPlayerService _audioPlayerService;
  final SleepTimerService _sleepTimerService;
  final MqttClientAdapter _clientAdapter;

  String _slug = 'absorb';
  StreamSubscription? _incomingSub;
  Timer? _heartbeatTimer;
  bool _isPlayerListenerAttached = false;
  bool _isSleepTimerListenerAttached = false;
  _PlayerSnapshot? _lastSnapshot;
  _SleepTimerSnapshot? _lastSleepSnapshot;

  bool get isConnected => _clientAdapter.isConnected;
  String get slug => _slug;
  String get statusTopic => 'absorb/$_slug/status';
  String get commandTopic => 'absorb/$_slug/set';
  String get stateTopic => 'absorb/$_slug/state';
  String get seekTopic => 'absorb/$_slug/seek/set';
  String get volumeTopic => 'absorb/$_slug/volume/set';
  String get sleepTimerTopic => 'absorb/$_slug/sleep_timer';
  String get sleepTimerSetTopic => 'absorb/$_slug/sleep_timer/set';

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

    // Subscribe to command and control topics
    _clientAdapter.subscribe(commandTopic);
    _clientAdapter.subscribe(seekTopic);
    _clientAdapter.subscribe(volumeTopic);
    _clientAdapter.subscribe(sleepTimerSetTopic);

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

    // Attach listener for sleep timer state changes
    if (!_isSleepTimerListenerAttached) {
      _sleepTimerService.addListener(onSleepTimerChanged);
      _isSleepTimerListenerAttached = true;
    }

    // Initial state publish
    onPlayerStateChanged(force: true);
    onSleepTimerChanged(force: true);

    return true;
  }

  Future<void> _handleInboundMessage(String topic, String payload) async {
    if (topic == commandTopic) {
      final command = payload.trim().toUpperCase();
      if (command == 'PLAY') {
        await _audioPlayerService.play(fromUi: false);
      } else if (command == 'PAUSE') {
        await _audioPlayerService.pause();
      } else if (command == 'SKIP_FORWARD') {
        final skipSec = await PlayerSettings.getForwardSkip();
        await _audioPlayerService.skipForward(skipSec);
        _publishState();
      } else if (command == 'SKIP_BACKWARD') {
        final skipSec = await PlayerSettings.getBackSkip();
        await _audioPlayerService.skipBackward(skipSec);
        _publishState();
      }
    } else if (topic == seekTopic) {
      final seconds = num.tryParse(payload.trim())?.toDouble();
      if (seconds != null && seconds >= 0) {
        await _audioPlayerService.seekTo(
          Duration(milliseconds: (seconds * 1000).round()),
        );
        _publishState();
      }
    } else if (topic == volumeTopic) {
      final val = num.tryParse(payload.trim())?.toDouble();
      if (val != null) {
        final targetVol =
            val > 1.0 ? (val / 100.0).clamp(0.0, 1.0) : val.clamp(0.0, 1.0);
        await _audioPlayerService.setVolume(targetVol);
        _publishState();
      }
    } else if (topic == sleepTimerSetTopic) {
      _handleSleepTimerCommand(payload);
    }
  }

  void _handleSleepTimerCommand(String rawPayload) {
    final trimmed = rawPayload.trim();
    if (trimmed.isEmpty) return;

    try {
      final decoded = jsonDecode(trimmed);
      if (decoded is! Map<String, dynamic>) return;

      if (decoded['cancel'] == true) {
        _sleepTimerService.cancel();
      } else if (decoded['mode'] == 'end_of_chapter') {
        _sleepTimerService.setChapterSleep(1);
      } else if (decoded['duration_minutes'] is num) {
        final minutes = (decoded['duration_minutes'] as num).toInt();
        if (minutes > 0) {
          _sleepTimerService.setTimeSleep(Duration(minutes: minutes));
        }
      }
    } catch (_) {
      // Malformed JSON is ignored
    }
  }

  Map<String, dynamic> buildSleepTimerPayload() {
    return {
      'active': _sleepTimerService.isActive,
      'mode': _sleepTimerService.mode.name,
      'remaining_seconds': _sleepTimerService.timeRemaining.inSeconds,
      'initial_minutes': _sleepTimerService.initialDuration.inMinutes,
    };
  }

  void onSleepTimerChanged({bool force = false}) {
    final payload = buildSleepTimerPayload();
    final active = payload['active'] as bool;
    final mode = payload['mode'] as String;
    final remainingSec = payload['remaining_seconds'] as int;
    final initialMin = payload['initial_minutes'] as int;

    final isStateTransition = force ||
        _lastSleepSnapshot == null ||
        _lastSleepSnapshot!.active != active ||
        _lastSleepSnapshot!.mode != mode ||
        _lastSleepSnapshot!.initialMinutes != initialMin ||
        remainingSec == 0;

    final isThrottledTick = _lastSleepSnapshot != null &&
        (_lastSleepSnapshot!.remainingSeconds - remainingSec).abs() >= 10;

    if (isStateTransition || isThrottledTick) {
      _lastSleepSnapshot = (
        active: active,
        mode: mode,
        remainingSeconds: remainingSec,
        initialMinutes: initialMin,
      );
      final encoded = jsonEncode(payload);
      _clientAdapter.publish(sleepTimerTopic, encoded, retain: true);
    }
  }

  Map<String, dynamic> buildStatePayload() {
    final isPlaying = _audioPlayerService.isPlaying;
    final currentState = isPlaying ? 'playing' : 'paused';
    final currentChapter = _audioPlayerService.currentChapter;
    final chapterTitle = currentChapter?['title'] as String?;
    final chapters = _audioPlayerService.chapters;
    int? chapterIndex;
    if (currentChapter != null) {
      final idx = chapters.indexWhere(
        (ch) =>
            identical(ch, currentChapter) ||
            (ch is Map &&
                ((ch['id'] != null && ch['id'] == currentChapter['id']) ||
                    (ch['title'] == currentChapter['title'] &&
                        ch['start'] == currentChapter['start']))),
      );
      if (idx >= 0) chapterIndex = idx;
    }

    return {
      'state': currentState,
      'title': _audioPlayerService.nowPlayingTitle,
      'author': _audioPlayerService.currentAuthor,
      'book': _audioPlayerService.currentTitle,
      'chapter_title': chapterTitle,
      'chapter_index': chapterIndex,
      'duration_seconds': _audioPlayerService.totalDuration,
      'position_seconds': _audioPlayerService.position.inMilliseconds / 1000.0,
      'volume': _audioPlayerService.volume,
      'speed': _audioPlayerService.speed,
      'cover_url': _audioPlayerService.currentCoverUrl,
    };
  }

  void _publishState() {
    final payload = jsonEncode(buildStatePayload());
    _clientAdapter.publish(stateTopic, payload, retain: false);
  }

  void onPlayerStateChanged({bool force = false}) {
    final isPlaying = _audioPlayerService.isPlaying;
    final book = _audioPlayerService.currentTitle;
    final chapter = _audioPlayerService.currentChapter?['title'] as String?;
    final speed = _audioPlayerService.speed;
    final posSec = _audioPlayerService.position.inMilliseconds / 1000.0;

    final isSeek = _lastSnapshot != null &&
        _lastSnapshot!.isPlaying &&
        isPlaying &&
        (posSec - _lastSnapshot!.positionSec).abs() > 2.0;

    final hasStateTransition = force ||
        _lastSnapshot?.isPlaying != isPlaying ||
        _lastSnapshot?.book != book ||
        _lastSnapshot?.chapter != chapter ||
        _lastSnapshot?.speed != speed ||
        isSeek;

    _lastSnapshot = (
      isPlaying: isPlaying,
      book: book,
      chapter: chapter,
      speed: speed,
      positionSec: posSec,
    );

    if (hasStateTransition) {
      _publishState();

      if (isPlaying) {
        _startHeartbeat();
      } else {
        _stopHeartbeat();
      }
    }
  }

  void _startHeartbeat() {
    _heartbeatTimer?.cancel();
    _heartbeatTimer = Timer.periodic(const Duration(seconds: 10), (_) {
      _publishState();
    });
  }

  void _stopHeartbeat() {
    _heartbeatTimer?.cancel();
    _heartbeatTimer = null;
  }

  void disconnect() {
    if (_clientAdapter.isConnected) {
      _clientAdapter.publish(statusTopic, 'offline', retain: true);
    }
    _stopHeartbeat();
    _incomingSub?.cancel();
    _incomingSub = null;
    if (_isPlayerListenerAttached) {
      _audioPlayerService.removeListener(onPlayerStateChanged);
      _isPlayerListenerAttached = false;
    }
    if (_isSleepTimerListenerAttached) {
      _sleepTimerService.removeListener(onSleepTimerChanged);
      _isSleepTimerListenerAttached = false;
    }
    _clientAdapter.disconnect();
    _lastSnapshot = null;
    _lastSleepSnapshot = null;
  }

  void dispose() {
    disconnect();
  }
}
