import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;
import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:flutter/foundation.dart';
import 'package:mqtt_client/mqtt_client.dart';
import 'package:mqtt_client/mqtt_server_client.dart';
import 'audio_player_service.dart';
import 'sleep_timer_service.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'api_service.dart';
import 'download_service.dart';
import 'user_account_service.dart';
import 'mqtt_settings.dart';


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
  void Function()? onDisconnected;
  bool get isConnected;
}

class DefaultMqttClientAdapter implements MqttClientAdapter {
  MqttServerClient? _client;
  final StreamController<({String topic, String payload})> _incomingController =
      StreamController<({String topic, String payload})>.broadcast();
  StreamSubscription? _updateSub;

  @override
  void Function()? onDisconnected;

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
        client.onDisconnected = () {
          onDisconnected?.call();
        };
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
      client.onDisconnected = null;
      client.disconnect();
    }
    return false;
  }

  @override
  void disconnect() {
    _updateSub?.cancel();
    if (_client != null) {
      _client!.onDisconnected = null;
      _client!.disconnect();
      _client = null;
    }
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

  void dispose() {
    disconnect();
    _incomingController.close();
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

enum MqttConnectionStatus {
  disconnected,
  connecting,
  connected,
  error,
}

class MqttRemoteService extends ChangeNotifier {
  static const double minSpeed = 0.5;
  static const double maxSpeed = 3.0;
  static const int minSleepDurationMinutes = 0;
  static const int maxSleepDurationMinutes = 120;
  static const int stepSleepDurationMinutes = 5;
  static const List<String> sleepTimerPresets = [
    'off',
    '15m',
    '30m',
    '45m',
    '60m',
    'end_of_chapter',
  ];
  static const Map<String, double> speedOptionValues = {
    '0.75x': 0.75,
    '1.0x': 1.0,
    '1.25x': 1.25,
    '1.5x': 1.5,
    '2.0x': 2.0,
  };
  static const List<String> speedOptions = [
    '0.75x',
    '1.0x',
    '1.25x',
    '1.5x',
    '2.0x',
  ];

  static MqttRemoteService _instance = MqttRemoteService._();
  factory MqttRemoteService() => _instance;

  @visibleForTesting
  static void setMockInstance(MqttRemoteService? instance) {
    _instance = instance ?? MqttRemoteService._();
  }
  MqttRemoteService._({
    Stream<List<ConnectivityResult>>? connectivityStream,
  })  : _audioPlayerService = AudioPlayerService(),
        _sleepTimerService = SleepTimerService(),
        _downloadService = DownloadService(),
        _apiProvider = null,
        _clientAdapter = DefaultMqttClientAdapter(),
        _random = math.Random(),
        _enableJitter = true {
    _clientAdapter.onDisconnected = _handleUnexpectedDisconnect;
    _subscribeConnectivity(
      connectivityStream ?? Connectivity().onConnectivityChanged,
    );
  }

  @visibleForTesting
  MqttRemoteService.forTesting({
    required AudioPlayerService audioPlayerService,
    SleepTimerService? sleepTimerService,
    DownloadService? downloadService,
    ApiService? Function()? apiProvider,
    required MqttClientAdapter clientAdapter,
    math.Random? random,
    bool enableJitter = true,
    Stream<List<ConnectivityResult>>? connectivityStream,
  })  : _audioPlayerService = audioPlayerService,
        _sleepTimerService = sleepTimerService ?? SleepTimerService(),
        _downloadService = downloadService ?? DownloadService(),
        _apiProvider = apiProvider,
        _clientAdapter = clientAdapter,
        _random = random ?? math.Random(0),
        _enableJitter = enableJitter {
    _clientAdapter.onDisconnected = _handleUnexpectedDisconnect;
    if (connectivityStream != null) {
      _subscribeConnectivity(connectivityStream);
    }
  }

  final AudioPlayerService _audioPlayerService;
  final SleepTimerService _sleepTimerService;
  final DownloadService _downloadService;
  final ApiService? Function()? _apiProvider;
  final MqttClientAdapter _clientAdapter;
  final math.Random _random;
  final bool _enableJitter;

  StreamSubscription<List<ConnectivityResult>>? _connectivitySub;
  bool _isNetworkOnline = false;

  @visibleForTesting
  bool get isNetworkOnline => _isNetworkOnline;

  void _subscribeConnectivity(Stream<List<ConnectivityResult>> stream) {
    _connectivitySub?.cancel();
    _connectivitySub = stream.listen((results) {
      _handleConnectivityChanged(results);
    });
  }

  Future<void> _handleConnectivityChanged(
    List<ConnectivityResult> results,
  ) async {
    final isOnline = results.any((r) =>
        r == ConnectivityResult.wifi ||
        r == ConnectivityResult.mobile ||
        r == ConnectivityResult.ethernet ||
        r == ConnectivityResult.vpn);
    final wasOffline = !_isNetworkOnline;
    _isNetworkOnline = isOnline;

    if (isOnline && wasOffline) {
      await _handleConnectivityRestored();
    }
  }

  bool _isDisposed = false;

  @visibleForTesting
  bool get isDisposed => _isDisposed;

  Future<void> _handleConnectivityRestored() async {
    if (_isDisposed) return;
    if (_connectionStatus != MqttConnectionStatus.disconnected &&
        _connectionStatus != MqttConnectionStatus.error) {
      return;
    }
    final enabled = await MqttSettings.isEnabled();
    if (!enabled || _isDisposed) {
      return;
    }
    if (_connectionStatus != MqttConnectionStatus.disconnected &&
        _connectionStatus != MqttConnectionStatus.error) {
      return;
    }

    debugPrint(
      '[MqttRemoteService] Network connectivity restored, triggering immediate reconnect',
    );
    _cancelReconnectTimer();
    _reconnectAttempts = 0;
    await _attemptReconnect();
  }

  Future<bool> _attemptReconnect() async {
    if (_isDisposed) return false;
    final success = await connectFromSettings();
    if (_isDisposed) return success;
    if (success) {
      _reconnectAttempts = 0;
    } else {
      _reconnectAttempts++;
      if (!_isExplicitlyDisconnected &&
          (_connectionStatus == MqttConnectionStatus.disconnected ||
              _connectionStatus == MqttConnectionStatus.error)) {
        _scheduleReconnect();
      }
    }
    return success;
  }

  MqttConnectionStatus _connectionStatus = MqttConnectionStatus.disconnected;
  MqttConnectionStatus get connectionStatus => _connectionStatus;

  String _slug = 'absorb';
  StreamSubscription? _incomingSub;
  Timer? _heartbeatTimer;
  Timer? _reconnectTimer;
  int _reconnectAttempts = 0;
  bool _isExplicitlyDisconnected = false;
  bool _isPlayerListenerAttached = false;
  bool _isSleepTimerListenerAttached = false;
  bool _isDownloadListenerAttached = false;
  _PlayerSnapshot? _lastSnapshot;
  _SleepTimerSnapshot? _lastSleepSnapshot;

  bool get isConnected => _clientAdapter.isConnected;
  bool get isReconnecting =>
      _reconnectTimer != null && _reconnectTimer!.isActive;
  int get reconnectAttempts => _reconnectAttempts;
  Timer? get reconnectTimer => _reconnectTimer;
  String get slug => _slug;
  String get statusTopic => 'absorb/$_slug/status';
  String get commandTopic => 'absorb/$_slug/set';
  String get stateTopic => 'absorb/$_slug/state';
  String get seekTopic => 'absorb/$_slug/seek/set';
  String get volumeTopic => 'absorb/$_slug/volume/set';
  String get speedTopic => 'absorb/$_slug/speed/set';
  String get downloadedItemsTopic => 'absorb/$_slug/downloaded_items';
  String get sleepTimerTopic => 'absorb/$_slug/sleep_timer';
  String get sleepTimerSetTopic => 'absorb/$_slug/sleep_timer/set';
  String get sleepTimerDurationSetTopic =>
      'absorb/$_slug/sleep_timer/duration/set';
  String get sleepTimerPresetSetTopic =>
      'absorb/$_slug/sleep_timer_preset/set';
  String get playMediaTopic => 'absorb/$_slug/play_media/set';
  String get discoveryMediaPlayerTopic =>
      'homeassistant/media_player/absorb_$_slug/config';
  String get discoverySleepTimerSensorTopic =>
      'homeassistant/sensor/absorb_${_slug}_sleep_timer/config';
  String get discoverySleepChapterButtonTopic =>
      'homeassistant/button/absorb_${_slug}_sleep_chapter/config';
  String get discoverySleepTimerNumberTopic =>
      'homeassistant/number/absorb_${_slug}_sleep_timer/config';
  String get discoverySleepTimerPresetSelectTopic =>
      'homeassistant/select/absorb_${_slug}_sleep_timer_preset/config';
  String get speedSelectSetTopic => 'absorb/$_slug/speed/select/set';
  String get discoverySpeedSelectTopic =>
      'homeassistant/select/absorb_${_slug}_speed/config';

  static String sanitizeSlug(String rawSlug) {
    final sanitized = rawSlug.trim().replaceAll(RegExp(r'[^a-zA-Z0-9_-]'), '_');
    return sanitized.isEmpty ? 'absorb' : sanitized.toLowerCase();
  }

  @visibleForTesting
  Duration computeReconnectDelay(int attempt) {
    final int baseSeconds;
    if (attempt <= 0) {
      baseSeconds = 2;
    } else if (attempt >= 5) {
      baseSeconds = 60;
    } else {
      baseSeconds = (2 * (1 << attempt)).clamp(2, 60);
    }
    final baseMs = baseSeconds * 1000;
    if (!_enableJitter) {
      return Duration(milliseconds: baseMs);
    }
    // Jitter: +/- 15% of base delay
    final jitterRange = (baseMs * 0.15).round();
    final randomOffset = (_random.nextDouble() * 2 - 1) * jitterRange;
    final totalMs = (baseMs + randomOffset).round().clamp(1000, 70000);
    return Duration(milliseconds: totalMs);
  }

  void _cancelReconnectTimer() {
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
  }

  void _teardownSession() {
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
    if (_isDownloadListenerAttached) {
      _downloadService.removeListener(onDownloadedItemsChanged);
      _isDownloadListenerAttached = false;
    }
    _lastSnapshot = null;
    _lastSleepSnapshot = null;
    _setConnectionStatus(MqttConnectionStatus.disconnected);
  }

  void _handleUnexpectedDisconnect() {
    if (_isExplicitlyDisconnected ||
        _connectionStatus == MqttConnectionStatus.disconnected) {
      return;
    }
    debugPrint('[MqttRemoteService] Socket disconnected unexpectedly');
    _teardownSession();
    _scheduleReconnect();
  }

  void _scheduleReconnect() {
    _cancelReconnectTimer();
    final delay = computeReconnectDelay(_reconnectAttempts);
    debugPrint(
      '[MqttRemoteService] Scheduling reconnect attempt $_reconnectAttempts in ${delay.inMilliseconds}ms',
    );
    _reconnectTimer = Timer(delay, () async {
      _reconnectTimer = null;
      if (_isExplicitlyDisconnected) return;

      final enabled = await MqttSettings.isEnabled();
      if (!enabled) {
        disconnect();
        return;
      }
      await _attemptReconnect();
    });
  }

  void _setConnectionStatus(MqttConnectionStatus status) {
    if (_isDisposed) return;
    if (_connectionStatus == status) return;
    _connectionStatus = status;
    notifyListeners();
  }

  Future<bool> connectFromSettings() async {
    if (_isDisposed) return false;
    final enabled = await MqttSettings.isEnabled();
    if (!enabled || _isDisposed) {
      disconnect();
      return false;
    }
    final host = await MqttSettings.getHost();
    if (host.isEmpty || _isDisposed) {
      disconnect();
      return false;
    }
    final port = await MqttSettings.getPort();
    final slug = await MqttSettings.getSlug();
    final username = await MqttSettings.getUsername();
    final password = await MqttSettings.getPassword();
    final discovery = await MqttSettings.isDiscoveryEnabled();
    final tls = await MqttSettings.useTls();

    if (_isDisposed) return false;

    return connect(
      host: host,
      port: port,
      slug: slug,
      username: username.isNotEmpty ? username : null,
      password: password.isNotEmpty ? password : null,
      useTls: tls,
      enableDiscovery: discovery,
    );
  }

  Future<bool> connect({
    required String host,
    int? port,
    required String slug,
    String? username,
    String? password,
    bool useTls = false,
    bool enableDiscovery = true,
  }) async {
    if (_isDisposed) return false;
    _cancelReconnectTimer();
    _isExplicitlyDisconnected = false;
    _setConnectionStatus(MqttConnectionStatus.connecting);
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

    try {
      final success = await _clientAdapter.connect(config);
      if (_isDisposed || _isExplicitlyDisconnected) {
        if (success) {
          _clientAdapter.disconnect();
        }
        return false;
      }
      if (!success) {
        _setConnectionStatus(MqttConnectionStatus.error);
        return false;
      }

      // Publish online status retained
      _clientAdapter.publish(statusTopic, 'online', retain: true);

      // Subscribe to command and control topics
      _clientAdapter.subscribe(commandTopic);
      _clientAdapter.subscribe(seekTopic);
      _clientAdapter.subscribe(volumeTopic);
      _clientAdapter.subscribe(speedTopic);
      _clientAdapter.subscribe(sleepTimerSetTopic);
      _clientAdapter.subscribe(sleepTimerDurationSetTopic);
      _clientAdapter.subscribe(sleepTimerPresetSetTopic);
      _clientAdapter.subscribe(speedSelectSetTopic);
      _clientAdapter.subscribe(playMediaTopic);

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

      // Attach listener for downloaded items changes
      if (!_isDownloadListenerAttached) {
        _downloadService.addListener(onDownloadedItemsChanged);
        _isDownloadListenerAttached = true;
      }

      // Initial state publish
      onPlayerStateChanged(force: true);
      onSleepTimerChanged(force: true);
      onDownloadedItemsChanged();

      if (enableDiscovery) {
        publishDiscovery();
      }

      _reconnectAttempts = 0;
      _setConnectionStatus(MqttConnectionStatus.connected);
      return true;
    } catch (e) {
      debugPrint('[MqttRemoteService] connect error: $e');
      _setConnectionStatus(MqttConnectionStatus.error);
      return false;
    }
  }

  Map<String, dynamic> buildDeviceMetadata() {
    final model = ApiService.deviceModel.isNotEmpty
        ? ApiService.deviceModel
        : 'Absorb Client';
    final manufacturer = ApiService.deviceManufacturer.isNotEmpty
        ? ApiService.deviceManufacturer
        : 'Absorb';
    final swVersion = ApiService.appVersionFull.isNotEmpty
        ? ApiService.appVersionFull
        : '1.0.0';

    return {
      'identifiers': ['absorb_$_slug'],
      'name': 'Absorb ($_slug)',
      'model': model,
      'manufacturer': manufacturer,
      'sw_version': swVersion,
    };
  }

  Map<String, dynamic> buildMediaPlayerDiscoveryPayload() {
    return {
      'name': 'Absorb ($_slug)',
      'unique_id': 'absorb_${_slug}_media_player',
      'state_topic': stateTopic,
      'command_topic': commandTopic,
      'seek_topic': seekTopic,
      'volume_command_topic': volumeTopic,
      'volume_state_topic': stateTopic,
      'volume_state_template': '{{ value_json.volume }}',
      'availability_topic': statusTopic,
      'payload_available': 'online',
      'payload_not_available': 'offline',
      'payload_play': 'PLAY',
      'payload_pause': 'PAUSE',
      'payload_stop': 'STOP',
      'payload_next_track': 'NEXT_CHAPTER',
      'payload_previous_track': 'PREV_CHAPTER',
      'supported_features': [
        'play',
        'pause',
        'stop',
        'seek',
        'volume_set',
        'next_track',
        'previous_track',
        'play_media',
      ],
      'device': buildDeviceMetadata(),
    };
  }

  Map<String, dynamic> buildSleepTimerSensorDiscoveryPayload() {
    return {
      'name': 'Absorb ($_slug) Sleep Timer',
      'unique_id': 'absorb_${_slug}_sleep_timer',
      'state_topic': sleepTimerTopic,
      'value_template': '{{ value_json.remaining_seconds }}',
      'unit_of_measurement': 's',
      'device_class': 'duration',
      'availability_topic': statusTopic,
      'payload_available': 'online',
      'payload_not_available': 'offline',
      'icon': 'mdi:timer-sand',
      'device': buildDeviceMetadata(),
    };
  }

  Map<String, dynamic> buildSleepChapterButtonDiscoveryPayload() {
    return {
      'name': 'Absorb ($_slug) Sleep End of Chapter',
      'unique_id': 'absorb_${_slug}_sleep_chapter',
      'command_topic': sleepTimerSetTopic,
      'payload_press': jsonEncode({'mode': 'end_of_chapter'}),
      'availability_topic': statusTopic,
      'payload_available': 'online',
      'payload_not_available': 'offline',
      'icon': 'mdi:timer-off-outline',
      'device': buildDeviceMetadata(),
    };
  }

  Map<String, dynamic> buildSleepTimerNumberDiscoveryPayload() {
    return {
      'name': 'Absorb ($_slug) Sleep Timer Duration',
      'unique_id': 'absorb_${_slug}_sleep_timer_duration',
      'command_topic': sleepTimerDurationSetTopic,
      'state_topic': sleepTimerTopic,
      'value_template':
          '{{ (value_json.remaining_seconds / 60) | round(0) if value_json.active else 0 }}',
      'min': minSleepDurationMinutes,
      'max': maxSleepDurationMinutes,
      'step': stepSleepDurationMinutes,
      'unit': 'min',
      'unit_of_measurement': 'min',
      'icon': 'mdi:timer-outline',
      'availability_topic': statusTopic,
      'payload_available': 'online',
      'payload_not_available': 'offline',
      'device': buildDeviceMetadata(),
    };
  }

  Map<String, dynamic> buildSleepTimerPresetSelectDiscoveryPayload() {
    return {
      'name': 'Absorb ($_slug) Sleep Timer Preset',
      'unique_id': 'absorb_${_slug}_sleep_timer_preset',
      'command_topic': sleepTimerPresetSetTopic,
      'state_topic': sleepTimerTopic,
      'value_template':
          "{{ 'end_of_chapter' if value_json.mode == 'end_of_chapter' else ((value_json.initial_minutes | string + 'm') if value_json.active else 'off') }}",
      'options': sleepTimerPresets,
      'icon': 'mdi:timer-cog-outline',
      'availability_topic': statusTopic,
      'payload_available': 'online',
      'payload_not_available': 'offline',
      'device': buildDeviceMetadata(),
    };
  }

  Map<String, dynamic> buildSpeedSelectDiscoveryPayload() {
    return {
      'name': 'Absorb ($_slug) Playback Speed',
      'unique_id': 'absorb_${_slug}_speed',
      'command_topic': speedSelectSetTopic,
      'state_topic': stateTopic,
      'value_template': "{{ value_json.speed | string + 'x' }}",
      'options': speedOptions,
      'icon': 'mdi:play-speed',
      'availability_topic': statusTopic,
      'payload_available': 'online',
      'payload_not_available': 'offline',
      'device': buildDeviceMetadata(),
    };
  }

  List<String> get allDiscoveryTopics => [
        discoveryMediaPlayerTopic,
        discoverySleepTimerSensorTopic,
        discoverySleepChapterButtonTopic,
        discoverySleepTimerNumberTopic,
        discoverySleepTimerPresetSelectTopic,
        discoverySpeedSelectTopic,
      ];

  void publishDiscovery() {
    _clientAdapter.publish(
      discoveryMediaPlayerTopic,
      jsonEncode(buildMediaPlayerDiscoveryPayload()),
      retain: true,
    );
    _clientAdapter.publish(
      discoverySleepTimerSensorTopic,
      jsonEncode(buildSleepTimerSensorDiscoveryPayload()),
      retain: true,
    );
    _clientAdapter.publish(
      discoverySleepChapterButtonTopic,
      jsonEncode(buildSleepChapterButtonDiscoveryPayload()),
      retain: true,
    );
    _clientAdapter.publish(
      discoverySleepTimerNumberTopic,
      jsonEncode(buildSleepTimerNumberDiscoveryPayload()),
      retain: true,
    );
    _clientAdapter.publish(
      discoverySleepTimerPresetSelectTopic,
      jsonEncode(buildSleepTimerPresetSelectDiscoveryPayload()),
      retain: true,
    );
    _clientAdapter.publish(
      discoverySpeedSelectTopic,
      jsonEncode(buildSpeedSelectDiscoveryPayload()),
      retain: true,
    );
  }

  void unpublishDiscovery() {
    for (final topic in allDiscoveryTopics) {
      _clientAdapter.publish(topic, '', retain: true);
    }
  }

  Future<void> _handleInboundMessage(String topic, String payload) async {
    if (topic == commandTopic) {
      final command = payload.trim().toUpperCase();
      if (command == 'PLAY') {
        await _audioPlayerService.play(fromUi: false);
      } else if (command == 'PAUSE') {
        await _audioPlayerService.pause();
      } else if (command == 'STOP') {
        await _audioPlayerService.stop();
      } else if (command == 'SKIP_FORWARD') {
        final skipSec = await PlayerSettings.getForwardSkip();
        await _audioPlayerService.skipForward(skipSec);
        _publishState();
      } else if (command == 'SKIP_BACKWARD') {
        final skipSec = await PlayerSettings.getBackSkip();
        await _audioPlayerService.skipBackward(skipSec);
        _publishState();
      } else if (command == 'NEXT_CHAPTER' || command == 'NEXT') {
        await _audioPlayerService.skipToNextChapter();
      } else if (command == 'PREV_CHAPTER' || command == 'PREVIOUS') {
        await _audioPlayerService.skipToPreviousChapter();
      } else if (command == 'PLAY_PAUSE') {
        if (_audioPlayerService.isPlaying) {
          await _audioPlayerService.pause();
        } else {
          await _audioPlayerService.play(fromUi: false);
        }
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
    } else if (topic == speedTopic) {
      final speed = num.tryParse(payload.trim())?.toDouble();
      if (speed != null) {
        await _applySpeed(speed);
      }
    } else if (topic == sleepTimerSetTopic) {
      _handleSleepTimerCommand(payload);
    } else if (topic == sleepTimerDurationSetTopic) {
      _handleSleepTimerDurationCommand(payload);
    } else if (topic == sleepTimerPresetSetTopic) {
      _handleSleepTimerPresetCommand(payload);
    } else if (topic == speedSelectSetTopic) {
      await _handleSpeedSelectCommand(payload);
    } else if (topic == playMediaTopic) {
      await _handlePlayMediaCommand(payload);
    }
  }

  Future<ApiService?> _getApiService() async {
    if (_apiProvider != null) {
      return _apiProvider();
    }
    if (_audioPlayerService.currentApi != null) {
      return _audioPlayerService.currentApi;
    }
    final prefs = await SharedPreferences.getInstance();
    final url = prefs.getString('server_url');
    final token = prefs.getString('token');
    final refreshToken = prefs.getString('refresh_token');
    final username = prefs.getString('username');
    if (url == null || token == null) return null;
    Map<String, String> customHeaders = const {};
    final headersJson = prefs.getString('custom_headers');
    if (headersJson != null && headersJson.isNotEmpty) {
      try {
        customHeaders =
            Map<String, String>.from(jsonDecode(headersJson) as Map);
      } catch (_) {}
    }
    return ApiService(
      baseUrl: url,
      token: token,
      refreshToken: refreshToken,
      isLegacyToken: refreshToken == null,
      customHeaders: customHeaders,
      loadPersistedTokens: () =>
          UserAccountService().loadPersistedTokens(url, username),
      onTokensRefreshed: (access, refresh) =>
          UserAccountService().persistRefreshedTokens(
        access,
        refresh,
        serverUrl: url,
        username: username,
      ),
    );
  }

  Future<void> _handlePlayMediaCommand(String rawPayload) async {
    final trimmed = rawPayload.trim();
    if (trimmed.isEmpty) return;

    final String itemId;
    final String? episodeId;

    try {
      final decoded = jsonDecode(trimmed);
      if (decoded is! Map<String, dynamic>) return;
      final rawItemId = decoded['item_id'];
      if (rawItemId is! String || rawItemId.trim().isEmpty) return;
      itemId = rawItemId.trim();
      final rawEpId = decoded['episode_id'];
      episodeId = (rawEpId is String && rawEpId.trim().isNotEmpty)
          ? rawEpId.trim()
          : null;
    } catch (_) {
      return;
    }

    try {
      final api = await _getApiService();
      if (api == null) {
        debugPrint('[MqttRemoteService] play_media: No API service available');
        return;
      }

      final dlKey = episodeId != null ? '$itemId-$episodeId' : itemId;
      if (_downloadService.isDownloaded(dlKey)) {
        final dl = _downloadService.getInfo(dlKey);
        double duration = 0.0;
        List<dynamic> chapters = [];
        if (dl.sessionData != null) {
          try {
            final session =
                jsonDecode(dl.sessionData!) as Map<String, dynamic>;
            duration = (session['duration'] as num?)?.toDouble() ?? 0.0;
            chapters = (session['chapters'] as List<dynamic>?) ?? [];
          } catch (_) {}
        }

        await _audioPlayerService.playItem(
          api: api,
          itemId: itemId,
          title: dl.title ?? '',
          author: dl.author ?? '',
          coverUrl: dl.coverUrl,
          totalDuration: duration,
          chapters: chapters,
          episodeId: episodeId,
          episodeTitle: episodeId != null ? dl.title : null,
          libraryId: dl.libraryId,
        );
        return;
      }

      final fullItem = await api.getLibraryItem(itemId);
      if (fullItem == null) {
        debugPrint(
            '[MqttRemoteService] play_media: getLibraryItem returned null');
        return;
      }

      final media = fullItem['media'] as Map<String, dynamic>? ?? {};
      final metadata = media['metadata'] as Map<String, dynamic>? ?? {};
      final showOrBookTitle = metadata['title'] as String? ?? '';
      final author = metadata['authorName'] as String? ?? '';
      final coverUrl = api.getCoverUrl(itemId, width: 400);
      final libraryId = fullItem['libraryId'] as String?;

      String playTitle = showOrBookTitle;
      String playAuthor = author;
      double playDuration = (media['duration'] as num?)?.toDouble() ?? 0.0;
      List<dynamic> playChapters = (media['chapters'] as List<dynamic>?) ?? [];
      String? episodeTitle;

      if (episodeId != null) {
        final episodes = (media['episodes'] as List<dynamic>?) ?? [];
        final ep = episodes.cast<Map<String, dynamic>?>().firstWhere(
          (e) => e?['id'] == episodeId,
          orElse: () => null,
        );
        if (ep == null) {
          debugPrint('[MqttRemoteService] play_media: Episode not found: $episodeId');
          return;
        }
        playTitle = ep['title'] as String? ?? showOrBookTitle;
        playAuthor = showOrBookTitle;
        playDuration = (ep['duration'] as num?)?.toDouble() ?? playDuration;
        playChapters = (ep['chapters'] as List<dynamic>?) ?? [];
        episodeTitle = playTitle;
      }

      await _audioPlayerService.playItem(
        api: api,
        itemId: itemId,
        title: playTitle,
        author: playAuthor,
        coverUrl: coverUrl,
        totalDuration: playDuration,
        chapters: playChapters,
        episodeId: episodeId,
        episodeTitle: episodeTitle,
        libraryId: libraryId,
      );
    } catch (e) {
      debugPrint('[MqttRemoteService] play_media failed: $e');
    }
  }

  void _handleSleepTimerCommand(String rawPayload) {
    final trimmed = rawPayload.trim();
    if (trimmed.isEmpty) return;

    if (trimmed.startsWith('{')) {
      try {
        final decoded = jsonDecode(trimmed);
        if (decoded is Map<String, dynamic>) {
          if (decoded['cancel'] == true) {
            _sleepTimerService.cancel();
          } else if (decoded['mode'] == 'end_of_chapter' || decoded['mode'] == 'chapter') {
            _sleepTimerService.setChapterSleep(1);
          } else if (decoded['duration_minutes'] is num) {
            final durationVal = decoded['duration_minutes'] as num;
            if (durationVal.isFinite && durationVal.toInt() > 0) {
              _sleepTimerService.setTimeSleep(Duration(minutes: durationVal.toInt()));
            }
          }
          return;
        }
      } catch (_) {
        // Fall through to non-JSON parsing
      }
    }

    var unquoted = trimmed;
    if (unquoted.startsWith('"') && unquoted.endsWith('"') && unquoted.length >= 2) {
      unquoted = unquoted.substring(1, unquoted.length - 1).trim();
    }

    final lower = unquoted.toLowerCase();
    if (lower == 'cancel') {
      _sleepTimerService.cancel();
      return;
    }

    if (lower == 'end_of_chapter' || lower == 'chapter') {
      _sleepTimerService.setChapterSleep(1);
      return;
    }

    final minutes = int.tryParse(unquoted);
    if (minutes != null && minutes > 0) {
      _sleepTimerService.setTimeSleep(Duration(minutes: minutes));
      return;
    }
  }

  void _handleSleepTimerDurationCommand(String rawPayload) {
    final trimmed = rawPayload.trim();
    if (trimmed.isEmpty) return;
    final val = num.tryParse(trimmed);
    if (val != null && val.isFinite) {
      final minutes = val.round();
      if (minutes == 0) {
        _sleepTimerService.cancel();
      } else if (minutes > 0) {
        _sleepTimerService.setTimeSleep(
          Duration(minutes: minutes.clamp(1, maxSleepDurationMinutes)),
        );
      }
    }
  }

  void _handleSleepTimerPresetCommand(String rawPayload) {
    final preset = rawPayload.trim().toLowerCase();
    switch (preset) {
      case 'off':
        _sleepTimerService.cancel();
      case '15m':
        _sleepTimerService.setTimeSleep(const Duration(minutes: 15));
      case '30m':
        _sleepTimerService.setTimeSleep(const Duration(minutes: 30));
      case '45m':
        _sleepTimerService.setTimeSleep(const Duration(minutes: 45));
      case '60m':
        _sleepTimerService.setTimeSleep(const Duration(minutes: 60));
      case 'end_of_chapter':
        _sleepTimerService.setChapterSleep(1);
    }
  }

  Future<void> _applySpeed(double speed) async {
    final clampedSpeed = speed.clamp(minSpeed, maxSpeed);
    await _audioPlayerService.setSpeed(clampedSpeed);
    _publishState();
  }

  Future<void> _handleSpeedSelectCommand(String rawPayload) async {
    final option = rawPayload.trim().toLowerCase();
    final rate = speedOptionValues[option];
    if (rate != null) {
      await _applySpeed(rate);
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

  List<Map<String, dynamic>> buildDownloadedItemsPayload() {
    final items = _downloadService.downloadedItems;
    return items.map((dl) {
      String itemId = dl.itemId;
      String? episodeId;
      if (dl.sessionData != null) {
        try {
          final session = jsonDecode(dl.sessionData!) as Map<String, dynamic>;
          if (session['episodeId'] is String) {
            episodeId = session['episodeId'] as String;
          }
        } catch (_) {}
      }
      if (episodeId == null && itemId.length > 36 && itemId[36] == '-') {
        episodeId = itemId.substring(37);
        itemId = itemId.substring(0, 36);
      }
      return {
        'item_id': itemId,
        if (episodeId != null) 'episode_id': episodeId,
        'title': dl.title ?? '',
        'author': dl.author ?? '',
        if (dl.coverUrl != null) 'cover_url': dl.coverUrl,
      };
    }).toList();
  }

  void onDownloadedItemsChanged() {
    final payload = jsonEncode(buildDownloadedItemsPayload());
    _clientAdapter.publish(downloadedItemsTopic, payload, retain: true);
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
    _isExplicitlyDisconnected = true;
    _cancelReconnectTimer();
    _reconnectAttempts = 0;
    if (_clientAdapter.isConnected) {
      _clientAdapter.publish(statusTopic, 'offline', retain: true);
    }
    _teardownSession();
    _clientAdapter.disconnect();
  }

  @override
  void dispose() {
    if (_isDisposed) return;
    disconnect();
    _isDisposed = true;
    _connectivitySub?.cancel();
    _connectivitySub = null;
    if (this != _instance) {
      super.dispose();
    }
  }
}
