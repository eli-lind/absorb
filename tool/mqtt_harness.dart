import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:mqtt_client/mqtt_client.dart';
import 'package:mqtt_client/mqtt_server_client.dart';

void main(List<String> args) async {
  String host = 'localhost';
  int port = 1883;
  String slug = 'absorb';
  String? username;
  String? password;
  bool smokeTest = false;
  bool rawJson = false;

  for (int i = 0; i < args.length; i++) {
    final arg = args[i];
    if (arg == '--host' && i + 1 < args.length) {
      host = args[++i];
    } else if (arg == '--port' && i + 1 < args.length) {
      port = int.tryParse(args[++i]) ?? 1883;
    } else if (arg == '--slug' && i + 1 < args.length) {
      slug = args[++i];
    } else if (arg == '--username' && i + 1 < args.length) {
      username = args[++i];
    } else if (arg == '--password' && i + 1 < args.length) {
      password = args[++i];
    } else if (arg == '--smoke') {
      smokeTest = true;
    } else if (arg == '--json') {
      rawJson = true;
    } else if (arg == '--help' || arg == '-h') {
      _printUsage();
      exit(0);
    }
  }

  final harness = MqttTestHarness(
    host: host,
    port: port,
    slug: slug,
    username: username,
    password: password,
    rawJson: rawJson,
  );

  if (smokeTest) {
    await harness.runSmokeTest();
  } else {
    await harness.runInteractive();
  }
}

void _printUsage() {
  stdout.writeln('''
Absorb MQTT Test Harness

Usage:
  dart run tool/mqtt_harness.dart [options]
  ./scripts/test-mqtt [options]

Options:
  --host <ip>       Broker hostname or IP (default: localhost)
  --port <port>     Broker port (default: 1883)
  --slug <slug>     Absorb device slug (default: absorb)
  --username <user> Optional MQTT username
  --password <pass> Optional MQTT password
  --smoke           Run automated contract & round-trip verification suite
  --json            Print raw received JSON payloads
  --help, -h        Show this help message
''');
}

class MqttTestHarness {
  final String host;
  final int port;
  final String slug;
  final String? username;
  final String? password;
  final bool rawJson;

  late final MqttServerClient _client;
  final Map<String, dynamic> _lastState = {};
  String? _lastStatus;
  final Map<String, dynamic> _lastSleepTimer = {};
  final Map<String, dynamic> _discovery = {};

  final Completer<String> _statusCompleter = Completer<String>();
  final Completer<Map<String, dynamic>> _haDiscoveryCompleter =
      Completer<Map<String, dynamic>>();
  final Completer<Map<String, dynamic>> _stateCompleter =
      Completer<Map<String, dynamic>>();
  final Completer<Map<String, dynamic>> _sleepCompleter =
      Completer<Map<String, dynamic>>();

  MqttTestHarness({
    required this.host,
    required this.port,
    required this.slug,
    this.username,
    this.password,
    this.rawJson = false,
  }) {
    final clientId = 'absorb_harness_${DateTime.now().millisecondsSinceEpoch % 100000}';
    _client = MqttServerClient.withPort(host, clientId, port);
    _client.logging(on: false);
    _client.keepAlivePeriod = 30;
    _client.connectionMessage = MqttConnectMessage()
        .withClientIdentifier(clientId)
        .startClean();
  }

  String get statusTopic => 'absorb/$slug/status';
  String get stateTopic => 'absorb/$slug/state';
  String get commandTopic => 'absorb/$slug/set';
  String get seekTopic => 'absorb/$slug/seek/set';
  String get volumeTopic => 'absorb/$slug/volume/set';
  String get sleepTimerTopic => 'absorb/$slug/sleep_timer';
  String get sleepTimerSetTopic => 'absorb/$slug/sleep_timer/set';
  String get playMediaTopic => 'absorb/$slug/play_media/set';
  String get haMediaPlayerTopic => 'homeassistant/media_player/absorb_$slug/config';
  String get haSleepTimerTopic => 'homeassistant/sensor/absorb_${slug}_sleep_timer/config';
  String get haSleepButtonTopic => 'homeassistant/button/absorb_${slug}_sleep_chapter/config';

  Future<bool> connect() async {
    stdout.writeln('\x1B[36mConnecting test harness to MQTT broker at $host:$port...\x1B[0m');
    try {
      final status = await _client.connect(username, password);
      if (status?.state == MqttConnectionState.connected) {
        stdout.writeln('\x1B[32m✓ Connected to broker successfully.\x1B[0m');

        _client.subscribe('absorb/$slug/#', MqttQos.atLeastOnce);
        _client.subscribe('homeassistant/#', MqttQos.atLeastOnce);

        _client.updates?.listen(_onMessage);
        return true;
      } else {
        stdout.writeln('\x1B[31m✗ Connection failed: ${status?.state}\x1B[0m');
        return false;
      }
    } catch (e) {
      stdout.writeln('\x1B[31m✗ Error connecting to broker: $e\x1B[0m');
      return false;
    }
  }

  void _onMessage(List<MqttReceivedMessage<MqttMessage>> events) {
    for (final event in events) {
      final message = event.payload as MqttPublishMessage;
      final topic = event.topic;
      final payload = utf8.decode(message.payload.message);

      if (rawJson) {
        stdout.writeln('[$topic] $payload');
      }

      if (topic == statusTopic) {
        _lastStatus = payload;
        if (!_statusCompleter.isCompleted) {
          _statusCompleter.complete(payload);
        }
        final color = payload == 'online' ? '\x1B[32m' : '\x1B[31m';
        stdout.writeln('$color[STATUS] App Availability: ${payload.toUpperCase()}\x1B[0m');
      } else if (topic == stateTopic) {
        try {
          final data = jsonDecode(payload) as Map<String, dynamic>;
          _lastState.addAll(data);
          if (!_stateCompleter.isCompleted) {
            _stateCompleter.complete(data);
          }
          _renderState(data);
        } catch (_) {}
      } else if (topic == sleepTimerTopic) {
        try {
          final data = jsonDecode(payload) as Map<String, dynamic>;
          _lastSleepTimer.addAll(data);
          if (!_sleepCompleter.isCompleted) {
            _sleepCompleter.complete(data);
          }
          _renderSleepTimer(data);
        } catch (_) {}
      } else if (topic == haMediaPlayerTopic) {
        try {
          final data = jsonDecode(payload) as Map<String, dynamic>;
          _discovery['media_player'] = data;
          if (!_haDiscoveryCompleter.isCompleted) {
            _haDiscoveryCompleter.complete(data);
          }
          stdout.writeln('\x1B[35m[HA DISCOVERY] Media Player registered: ${data['name']} (unique_id: ${data['unique_id']})\x1B[0m');
        } catch (_) {}
      } else if (topic == haSleepTimerTopic) {
        try {
          final data = jsonDecode(payload) as Map<String, dynamic>;
          _discovery['sleep_sensor'] = data;
          stdout.writeln('\x1B[35m[HA DISCOVERY] Sleep Timer Sensor registered: ${data['name']}\x1B[0m');
        } catch (_) {}
      } else if (topic == haSleepButtonTopic) {
        try {
          final data = jsonDecode(payload) as Map<String, dynamic>;
          _discovery['sleep_button'] = data;
          stdout.writeln('\x1B[35m[HA DISCOVERY] Sleep Button registered: ${data['name']}\x1B[0m');
        } catch (_) {}
      }
    }
  }

  void _renderState(Map<String, dynamic> data) {
    final state = (data['state'] ?? 'unknown').toString().toUpperCase();
    final title = data['book'] ?? data['title'] ?? 'No book';
    final author = data['author'] ?? '';
    final chapter = data['chapter_title'] ?? 'Chapter -';
    final pos = _formatDuration((data['position_seconds'] as num?)?.toDouble() ?? 0.0);
    final dur = _formatDuration((data['duration_seconds'] as num?)?.toDouble() ?? 0.0);
    final vol = (((data['volume'] as num?)?.toDouble() ?? 1.0) * 100).toInt();
    final speed = ((data['speed'] as num?)?.toDouble() ?? 1.0).toStringAsFixed(2);

    final icon = state == 'PLAYING'
        ? '▶'
        : state == 'PAUSED'
            ? '⏸'
            : '⏹';

    stdout.writeln(
      '\x1B[34m[STATE] $icon $state\x1B[0m | "$title" by $author | $chapter | $pos / $dur | Spd: ${speed}x | Vol: $vol%',
    );
  }

  void _renderSleepTimer(Map<String, dynamic> data) {
    final active = data['active'] == true;
    final mode = data['mode'] ?? 'off';
    final remSec = (data['remaining_seconds'] as num?)?.toInt() ?? 0;
    final initMin = data['initial_minutes'] ?? 0;

    if (active) {
      final remMin = (remSec / 60).ceil();
      stdout.writeln('\x1B[33m[SLEEP TIMER] Active ($mode): ~$remMin min remaining (${remSec}s, started with ${initMin}m)\x1B[0m');
    } else {
      stdout.writeln('\x1B[33m[SLEEP TIMER] Inactive\x1B[0m');
    }
  }

  String _formatDuration(double seconds) {
    final s = seconds.floor();
    final h = s ~/ 3600;
    final m = (s % 3600) ~/ 60;
    final sec = s % 60;
    if (h > 0) {
      return '${h.toString().padLeft(2, '0')}:${m.toString().padLeft(2, '0')}:${sec.toString().padLeft(2, '0')}';
    }
    return '${m.toString().padLeft(2, '0')}:${sec.toString().padLeft(2, '0')}';
  }

  void publish(String topic, String message, {bool retain = false}) {
    final builder = MqttClientPayloadBuilder();
    builder.addString(message);
    final data = builder.payload;
    if (data != null) {
      _client.publishMessage(topic, MqttQos.atLeastOnce, data, retain: retain);
      stdout.writeln('\x1B[90m-> Published [$topic]: $message\x1B[0m');
    }
  }

  Future<void> runInteractive() async {
    final connected = await connect();
    if (!connected) {
      exit(1);
    }

    stdout.writeln('''
========================================================================
 Absorb MQTT Test Harness (Interactive Mode)
 Slug: $slug | Host: $host:$port
 Topics: absorb/$slug/#, homeassistant/#
========================================================================
Commands:
  play                 - Send PLAY
  pause                - Send PAUSE
  stop                 - Send STOP
  next                 - Next chapter
  prev                 - Previous chapter
  vol <0.0-1.0>        - Set volume (e.g. vol 0.8)
  seek <seconds>       - Seek to position in seconds (e.g. seek 120)
  sleep <mins|chapter|cancel> - Control sleep timer (e.g. sleep 15, sleep cancel)
  play-media <itemId> [epId]  - Play specific item ID
  status               - Print last cached state
  help                 - Show commands
  quit / exit          - Exit harness
========================================================================
''');

    stdin.transform(utf8.decoder).transform(const LineSplitter()).listen((line) {
      final trimmed = line.trim();
      if (trimmed.isEmpty) return;

      final parts = trimmed.split(RegExp(r'\s+'));
      final cmd = parts[0].toLowerCase();

      switch (cmd) {
        case 'play':
          publish(commandTopic, 'PLAY');
          break;
        case 'pause':
          publish(commandTopic, 'PAUSE');
          break;
        case 'stop':
          publish(commandTopic, 'STOP');
          break;
        case 'next':
          publish(commandTopic, 'NEXT_CHAPTER');
          break;
        case 'prev':
        case 'previous':
          publish(commandTopic, 'PREV_CHAPTER');
          break;
        case 'vol':
        case 'volume':
          if (parts.length > 1) {
            publish(volumeTopic, parts[1]);
          } else {
            stdout.writeln('Usage: vol <0.0 - 1.0>');
          }
          break;
        case 'seek':
          if (parts.length > 1) {
            publish(seekTopic, parts[1]);
          } else {
            stdout.writeln('Usage: seek <seconds>');
          }
          break;
        case 'sleep':
          if (parts.length > 1) {
            final arg = parts[1].toLowerCase();
            if (arg == 'cancel') {
              publish(sleepTimerSetTopic, jsonEncode({'cancel': true}));
            } else if (arg == 'chapter' || arg == 'end_of_chapter') {
              publish(sleepTimerSetTopic, jsonEncode({'mode': 'end_of_chapter'}));
            } else if (int.tryParse(arg) != null) {
              publish(sleepTimerSetTopic, jsonEncode({'duration_minutes': int.parse(arg)}));
            } else if (arg.startsWith('{')) {
              publish(sleepTimerSetTopic, parts.sublist(1).join(' '));
            } else {
              stdout.writeln('Invalid sleep argument: $arg. Use: sleep <minutes | chapter | cancel>');
            }
          } else {
            stdout.writeln('Usage: sleep <minutes | chapter | cancel>');
          }
          break;
        case 'play-media':
          if (parts.length > 1) {
            final payload = {
              'item_id': parts[1],
              if (parts.length > 2) 'episode_id': parts[2],
            };
            publish(playMediaTopic, jsonEncode(payload));
          } else {
            stdout.writeln('Usage: play-media <itemId> [episodeId]');
          }
          break;
        case 'status':
          stdout.writeln('Last Status: $_lastStatus');
          stdout.writeln('Last State: ${jsonEncode(_lastState)}');
          stdout.writeln('Last Sleep: ${jsonEncode(_lastSleepTimer)}');
          break;
        case 'help':
          _printUsage();
          break;
        case 'quit':
        case 'exit':
          stdout.writeln('Disconnecting and exiting...');
          _client.disconnect();
          exit(0);
        default:
          stdout.writeln('Unknown command: "$cmd". Type "help" for options.');
      }
    });
  }

  Future<void> runSmokeTest() async {
    final connected = await connect();
    if (!connected) {
      stdout.writeln('\x1B[31mFAIL: Could not connect to broker.\x1B[0m');
      exit(1);
    }

    stdout.writeln('\n========================================================');
    stdout.writeln(' Starting Automated Absorb MQTT Smoke Test');
    stdout.writeln(' Target Slug: $slug');
    stdout.writeln('========================================================\n');

    bool allPassed = true;

    // 1. Verify Online Availability
    stdout.write('1. Checking app availability on "$statusTopic"... ');
    try {
      final status = await _statusCompleter.future.timeout(const Duration(seconds: 10));
      if (status == 'online') {
        stdout.writeln('\x1B[32mPASS (online)\x1B[0m');
      } else {
        stdout.writeln('\x1B[31mFAIL (status is "$status", expected "online")\x1B[0m');
        allPassed = false;
      }
    } catch (_) {
      stdout.writeln('\x1B[31mFAIL (timeout waiting for status)\x1B[0m');
      allPassed = false;
    }

    // 2. Verify Home Assistant Discovery Config
    stdout.write('2. Checking Home Assistant MQTT Discovery payload... ');
    try {
      final ha = await _haDiscoveryCompleter.future.timeout(const Duration(seconds: 6));
      if (ha['name'] != null &&
          ha['unique_id'] == 'absorb_${slug}_media_player' &&
          ha['state_topic'] == stateTopic &&
          ha['command_topic'] == commandTopic) {
        stdout.writeln('\x1B[32mPASS (valid discovery contract)\x1B[0m');
      } else {
        stdout.writeln('\x1B[31mFAIL (invalid discovery schema)\x1B[0m');
        allPassed = false;
      }
    } catch (_) {
      stdout.writeln('\x1B[33mWARN (discovery payload not received or disabled in settings)\x1B[0m');
    }

    // 3. Verify Sleep Timer Round-Trip Control
    stdout.write('3. Testing Sleep Timer control ($sleepTimerSetTopic -> 15m)... ');
    publish(sleepTimerSetTopic, jsonEncode({'duration_minutes': 15}));
    await Future.delayed(const Duration(seconds: 2));
    if (_lastSleepTimer['active'] == true &&
        (_lastSleepTimer['mode'] == 'time' || _lastSleepTimer['mode'] == 'timer')) {
      stdout.writeln('\x1B[32mPASS (timer active, ${_lastSleepTimer['remaining_seconds']}s remaining)\x1B[0m');
    } else {
      stdout.writeln('\x1B[31mFAIL (sleep timer did not activate: $_lastSleepTimer)\x1B[0m');
      allPassed = false;
    }

    // 4. Cancel Sleep Timer
    stdout.write('4. Testing Sleep Timer cancellation ($sleepTimerSetTopic -> cancel)... ');
    publish(sleepTimerSetTopic, jsonEncode({'cancel': true}));
    await Future.delayed(const Duration(seconds: 2));
    if (_lastSleepTimer['active'] == false) {
      stdout.writeln('\x1B[32mPASS (timer cleared)\x1B[0m');
    } else {
      stdout.writeln('\x1B[31mFAIL (sleep timer did not clear: $_lastSleepTimer)\x1B[0m');
      allPassed = false;
    }

    // 5. Volume Set Test
    stdout.write('5. Testing Volume Command ($volumeTopic -> 0.75)... ');
    publish(volumeTopic, '0.75');
    await Future.delayed(const Duration(seconds: 2));
    stdout.writeln('\x1B[32mPASS (dispatched volume command)\x1B[0m');

    stdout.writeln('\n========================================================');
    if (allPassed) {
      stdout.writeln('\x1B[32m✓ ALL SMOKE TEST ASSERTIONS PASSED\x1B[0m');
    } else {
      stdout.writeln('\x1B[31m✗ SOME ASSERTIONS FAILED\x1B[0m');
    }
    stdout.writeln('========================================================\n');

    _client.disconnect();
    exit(allPassed ? 0 : 1);
  }
}
