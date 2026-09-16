import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mocktail/mocktail.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:package_info_plus/package_info_plus.dart';
import 'package:absorb/services/audio_player_service.dart';
import 'package:absorb/services/backup_service.dart';
import 'package:absorb/services/mqtt_remote_service.dart';
import 'package:absorb/services/mqtt_settings.dart';
import 'package:absorb/services/scoped_prefs.dart';
import 'package:absorb/services/user_account_service.dart';
import 'package:absorb/screens/mqtt_settings_screen.dart';

class MockAudioPlayerService extends Mock implements AudioPlayerService {}

class FakeMqttClientAdapter implements MqttClientAdapter {
  MqttConnectionConfig? lastConfig;
  bool shouldSucceed = true;
  bool _isConnected = false;

  @override
  Stream<({String topic, String payload})> get incomingMessages =>
      const Stream.empty();

  @override
  bool get isConnected => _isConnected;

  @override
  Future<bool> connect(MqttConnectionConfig config) async {
    lastConfig = config;
    _isConnected = shouldSucceed;
    return shouldSucceed;
  }

  @override
  void disconnect() {
    _isConnected = false;
  }

  @override
  void Function()? onDisconnected;

  @override
  void subscribe(String topic) {}

  @override
  void publish(String topic, String payload, {bool retain = false}) {}
}

void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  final defaultSlugPattern = RegExp(r'^absorb_[0-9a-f]{4}$');

  setUp(() {
    SharedPreferences.setMockInitialValues({});
    PackageInfo.setMockInitialValues(
      appName: 'Absorb',
      packageName: 'com.absorb',
      version: '1.0.0',
      buildNumber: '1',
      buildSignature: '',
    );
  });

  group('MqttSettings', () {

    test('defaults match specifications', () async {
      expect(await MqttSettings.isEnabled(), isFalse);
      expect(await MqttSettings.getHost(), isEmpty);
      expect(await MqttSettings.getPort(), equals(1883));
      expect(await MqttSettings.getUsername(), isEmpty);
      expect(await MqttSettings.getPassword(), isEmpty);
      expect(await MqttSettings.getSlug(), matches(defaultSlugPattern));
      expect(await MqttSettings.isDiscoveryEnabled(), isTrue);
      expect(await MqttSettings.useTls(), isFalse);
    });

    test('generates unique 4-hex slug and persists immediately when unset', () async {
      final prefs = await SharedPreferences.getInstance();
      expect(prefs.containsKey(MqttSettings.keySlug), isFalse);

      final slug1 = await MqttSettings.getSlug();
      expect(slug1, matches(defaultSlugPattern));
      expect(prefs.getString(MqttSettings.keySlug), equals(slug1));

      // Subsequent calls return the identical persisted slug
      final slug2 = await MqttSettings.getSlug();
      expect(slug2, equals(slug1));
    });

    test('concurrent getSlug calls resolve to identical generated slug without race', () async {
      final results = await Future.wait([
        MqttSettings.getSlug(),
        MqttSettings.getSlug(),
        MqttSettings.getSlug(),
      ]);

      expect(results[0], matches(defaultSlugPattern));
      expect(results[1], equals(results[0]));
      expect(results[2], equals(results[0]));
      final prefs = await SharedPreferences.getInstance();
      expect(prefs.getString(MqttSettings.keySlug), equals(results[0]));
    });

    test('generates and persists unique 4-hex slug when existing slug is empty', () async {
      final prefs = await SharedPreferences.getInstance();
      await prefs.setString(MqttSettings.keySlug, '');

      final slug = await MqttSettings.getSlug();
      expect(slug, matches(defaultSlugPattern));
      expect(prefs.getString(MqttSettings.keySlug), equals(slug));
    });

    test('preserves pre-existing legacy absorb slug unchanged', () async {
      final prefs = await SharedPreferences.getInstance();
      await prefs.setString(MqttSettings.keySlug, 'absorb');

      final slug = await MqttSettings.getSlug();
      expect(slug, equals('absorb'));
      expect(prefs.getString(MqttSettings.keySlug), equals('absorb'));
    });

    test('preserves pre-existing custom slug unchanged', () async {
      final prefs = await SharedPreferences.getInstance();
      await prefs.setString(MqttSettings.keySlug, 'bedroom_tablet');

      final slug = await MqttSettings.getSlug();
      expect(slug, equals('bedroom_tablet'));
      expect(prefs.getString(MqttSettings.keySlug), equals('bedroom_tablet'));
    });

    test('persists and retrieves all settings', () async {
      await MqttSettings.setEnabled(true);
      await MqttSettings.setHost('192.168.1.100');
      await MqttSettings.setPort(8883);
      await MqttSettings.setUsername('tablet_user');
      await MqttSettings.setPassword('secret123');
      await MqttSettings.setSlug('kids_bedroom');
      await MqttSettings.setDiscoveryEnabled(false);
      await MqttSettings.setUseTls(true);

      expect(await MqttSettings.isEnabled(), isTrue);
      expect(await MqttSettings.getHost(), equals('192.168.1.100'));
      expect(await MqttSettings.getPort(), equals(8883));
      expect(await MqttSettings.getUsername(), equals('tablet_user'));
      expect(await MqttSettings.getPassword(), equals('secret123'));
      expect(await MqttSettings.getSlug(), equals('kids_bedroom'));
      expect(await MqttSettings.isDiscoveryEnabled(), isFalse);
      expect(await MqttSettings.useTls(), isTrue);
    });

    test('toMap and fromMap serialize and deserialize correctly', () async {
      final input = {
        'enabled': true,
        'host': 'mqtt.home.arpa',
        'port': 1883,
        'username': 'homeassistant',
        'password': 'ha-password',
        'slug': 'living_room_tablet',
        'discoveryEnabled': true,
        'useTls': false,
      };

      await MqttSettings.fromMap(input);
      final output = await MqttSettings.toMap();

      expect(output, equals(input));
    });
  });

  group('ScopedPrefs & Global Persistence', () {
    test('ScopedPrefs._globalKeys contains all MQTT keys', () {
      final globalKeys = ScopedPrefs.globalKeys;
      expect(globalKeys, contains(MqttSettings.keyEnabled));
      expect(globalKeys, contains(MqttSettings.keyHost));
      expect(globalKeys, contains(MqttSettings.keyPort));
      expect(globalKeys, contains(MqttSettings.keyUsername));
      expect(globalKeys, contains(MqttSettings.keyPassword));
      expect(globalKeys, contains(MqttSettings.keySlug));
      expect(globalKeys, contains(MqttSettings.keyDiscoveryEnabled));
      expect(globalKeys, contains(MqttSettings.keyUseTls));
    });

    test('ScopedPrefs.migrateToScope preserves MQTT keys at global level', () async {
      final prefs = await SharedPreferences.getInstance();
      await prefs.setBool(MqttSettings.keyEnabled, true);
      await prefs.setString(MqttSettings.keyHost, '10.0.0.5');
      await prefs.setString('normal_user_setting', 'user_value');

      // Emulate user scope
      await UserAccountService().saveAccount(
        SavedAccount(
          serverUrl: 'https://example.com',
          username: 'alice',
          token: 'token-123',
          userId: 'user-1',
        ),
      );

      await ScopedPrefs.migrateToScope();

      final scope = UserAccountService().activeScopeKey;
      expect(prefs.getBool(MqttSettings.keyEnabled), isTrue);
      expect(prefs.getString(MqttSettings.keyHost), equals('10.0.0.5'));
      // Scoped migration should NOT have prefixed MQTT keys with user scope
      expect(prefs.containsKey('$scope:${MqttSettings.keyEnabled}'), isFalse);
      expect(prefs.containsKey('$scope:${MqttSettings.keyHost}'), isFalse);
      // But normal settings should have been scoped
      expect(prefs.getString('$scope:normal_user_setting'), equals('user_value'));
    });
  });

  group('BackupService Integration', () {
    test('exportSettings includes mqtt configuration', () async {
      await MqttSettings.setEnabled(true);
      await MqttSettings.setHost('192.168.1.200');
      await MqttSettings.setPort(1883);
      await MqttSettings.setSlug('tablet_a7');

      final exported = await BackupService.exportSettings(includeAccounts: false);

      expect(exported.containsKey('mqtt'), isTrue);
      final mqttMap = exported['mqtt'] as Map<String, dynamic>;
      expect(mqttMap['enabled'], isTrue);
      expect(mqttMap['host'], equals('192.168.1.200'));
      expect(mqttMap['port'], equals(1883));
      expect(mqttMap['slug'], equals('tablet_a7'));
    });

    test('importSettings restores mqtt configuration', () async {
      final backupData = <String, dynamic>{
        'version': 3,
        'settings': <String, dynamic>{},
        'mqtt': <String, dynamic>{
          'enabled': true,
          'host': '192.168.0.10',
          'port': 8883,
          'username': 'backup_user',
          'password': 'backup_password',
          'slug': 'fleet_tablet_01',
          'discoveryEnabled': false,
          'useTls': true,
        },
      };

      await BackupService.importSettings(backupData);

      expect(await MqttSettings.isEnabled(), isTrue);
      expect(await MqttSettings.getHost(), equals('192.168.0.10'));
      expect(await MqttSettings.getPort(), equals(8883));
      expect(await MqttSettings.getUsername(), equals('backup_user'));
      expect(await MqttSettings.getPassword(), equals('backup_password'));
      expect(await MqttSettings.getSlug(), equals('fleet_tablet_01'));
      expect(await MqttSettings.isDiscoveryEnabled(), isFalse);
      expect(await MqttSettings.useTls(), isTrue);
    });
  });

  group('MqttRemoteService connection status and settings integration', () {
    late MockAudioPlayerService mockAudioPlayer;
    late FakeMqttClientAdapter fakeMqttClient;
    late MqttRemoteService service;

    setUp(() {
      mockAudioPlayer = MockAudioPlayerService();
      fakeMqttClient = FakeMqttClientAdapter();

      when(() => mockAudioPlayer.isPlaying).thenReturn(false);
      when(() => mockAudioPlayer.nowPlayingTitle).thenReturn('');
      when(() => mockAudioPlayer.currentTitle).thenReturn('');
      when(() => mockAudioPlayer.currentAuthor).thenReturn('');
      when(() => mockAudioPlayer.currentCoverUrl).thenReturn(null);
      when(() => mockAudioPlayer.totalDuration).thenReturn(0.0);
      when(() => mockAudioPlayer.position).thenReturn(Duration.zero);
      when(() => mockAudioPlayer.volume).thenReturn(1.0);
      when(() => mockAudioPlayer.speed).thenReturn(1.0);
      when(() => mockAudioPlayer.chapters).thenReturn([]);
      when(() => mockAudioPlayer.currentChapter).thenReturn(null);

      service = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayer,
        clientAdapter: fakeMqttClient,
      );
    });

    tearDown(() {
      service.dispose();
    });

    test('transitions through connecting, connected, and disconnected states', () async {
      final statuses = <MqttConnectionStatus>[];
      service.addListener(() {
        statuses.add(service.connectionStatus);
      });

      expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));

      final success = await service.connect(
        host: '192.168.1.50',
        slug: 'tablet',
      );

      expect(success, isTrue);
      expect(service.connectionStatus, equals(MqttConnectionStatus.connected));
      expect(statuses, containsAllInOrder([
        MqttConnectionStatus.connecting,
        MqttConnectionStatus.connected,
      ]));

      service.disconnect();
      expect(service.connectionStatus, equals(MqttConnectionStatus.disconnected));
    });

    test('transitions to error status on connection failure', () async {
      fakeMqttClient.shouldSucceed = false;

      final success = await service.connect(
        host: 'invalid.host',
        slug: 'tablet',
      );

      expect(success, isFalse);
      expect(service.connectionStatus, equals(MqttConnectionStatus.error));
    });

    test('connectFromSettings connects using persisted MqttSettings', () async {
      await MqttSettings.setEnabled(true);
      await MqttSettings.setHost('192.168.1.75');
      await MqttSettings.setPort(1883);
      await MqttSettings.setSlug('custom_slug');
      await MqttSettings.setUsername('alice');
      await MqttSettings.setPassword('pwd');
      await MqttSettings.setDiscoveryEnabled(true);

      final result = await service.connectFromSettings();

      expect(result, isTrue);
      expect(fakeMqttClient.lastConfig?.host, equals('192.168.1.75'));
      expect(fakeMqttClient.lastConfig?.port, equals(1883));
      expect(fakeMqttClient.lastConfig?.username, equals('alice'));
      expect(fakeMqttClient.lastConfig?.password, equals('pwd'));
      expect(service.slug, equals('custom_slug'));
    });
  });

  group('MqttSettingsScreen widget tests', () {
    late MockAudioPlayerService mockAudioPlayer;
    late FakeMqttClientAdapter fakeMqttClient;
    late MqttRemoteService testService;

    setUp(() {
      mockAudioPlayer = MockAudioPlayerService();
      fakeMqttClient = FakeMqttClientAdapter();

      when(() => mockAudioPlayer.isPlaying).thenReturn(false);
      when(() => mockAudioPlayer.nowPlayingTitle).thenReturn('');
      when(() => mockAudioPlayer.currentTitle).thenReturn('');
      when(() => mockAudioPlayer.currentAuthor).thenReturn('');
      when(() => mockAudioPlayer.currentCoverUrl).thenReturn(null);
      when(() => mockAudioPlayer.totalDuration).thenReturn(0.0);
      when(() => mockAudioPlayer.position).thenReturn(Duration.zero);
      when(() => mockAudioPlayer.volume).thenReturn(1.0);
      when(() => mockAudioPlayer.speed).thenReturn(1.0);
      when(() => mockAudioPlayer.chapters).thenReturn([]);
      when(() => mockAudioPlayer.currentChapter).thenReturn(null);

      testService = MqttRemoteService.forTesting(
        audioPlayerService: mockAudioPlayer,
        clientAdapter: fakeMqttClient,
      );
    });

    tearDown(() {
      testService.dispose();
    });

    testWidgets('renders all fields and status pill correctly', (tester) async {
      tester.view.physicalSize = const Size(1200, 1800);
      tester.view.devicePixelRatio = 1.0;
      addTearDown(() {
        tester.view.resetPhysicalSize();
        tester.view.resetDevicePixelRatio();
      });

      await MqttSettings.setEnabled(true);
      await MqttSettings.setHost('192.168.1.50');
      await MqttSettings.setPort(1883);
      await MqttSettings.setSlug('tablet_one');

      await tester.pumpWidget(
        MaterialApp(
          home: MqttSettingsScreen(mqttService: testService),
        ),
      );
      await tester.pump();
      await tester.pump(const Duration(milliseconds: 100));

      expect(find.text('Remote Control (MQTT)'), findsOneWidget);
      expect(find.text('Enable Remote Control'), findsOneWidget);
      expect(find.text('Broker Host or IP'), findsOneWidget);
      expect(find.text('192.168.1.50'), findsOneWidget);
      expect(find.text('tablet_one'), findsOneWidget);
      expect(find.text('Home Assistant MQTT Discovery'), findsOneWidget);
      expect(find.text('Save & Connect'), findsOneWidget);
    });

    testWidgets('saving valid settings persists values and invokes connect', (tester) async {
      tester.view.physicalSize = const Size(1200, 1800);
      tester.view.devicePixelRatio = 1.0;
      addTearDown(() {
        tester.view.resetPhysicalSize();
        tester.view.resetDevicePixelRatio();
      });

      await tester.pumpWidget(
        MaterialApp(
          home: MqttSettingsScreen(mqttService: testService),
        ),
      );
      await tester.pump();
      await tester.pump(const Duration(milliseconds: 100));

      // Toggle enable
      await tester.tap(find.text('Enable Remote Control'));
      await tester.pump();
      await tester.pump(const Duration(milliseconds: 100));

      // Enter host
      final hostField = find.widgetWithText(TextFormField, 'Broker Host or IP');
      await tester.enterText(hostField, '192.168.1.88');
      await tester.pump();

      // Enter slug
      final slugField = find.widgetWithText(TextFormField, 'Device Slug');
      await tester.enterText(slugField, 'my_new_slug');
      await tester.pump();

      // Scroll to and tap Save & Connect
      final saveBtn = find.widgetWithText(FilledButton, 'Save & Connect');
      await tester.ensureVisible(saveBtn);
      await tester.pump();
      await tester.pump(const Duration(milliseconds: 100));
      await tester.tap(saveBtn);
      await tester.pump();
      await tester.pump(const Duration(milliseconds: 100));

      // Drain toast timer
      await tester.pump(const Duration(seconds: 4));

      expect(await MqttSettings.isEnabled(), isTrue);
      expect(await MqttSettings.getHost(), equals('192.168.1.88'));
      expect(await MqttSettings.getSlug(), equals('my_new_slug'));
      expect(testService.connectionStatus, equals(MqttConnectionStatus.connected));
    });

    testWidgets('populates slug field with auto-generated slug on clean launch and saves modifications', (tester) async {
      tester.view.physicalSize = const Size(1200, 1800);
      tester.view.devicePixelRatio = 1.0;
      addTearDown(() {
        tester.view.resetPhysicalSize();
        tester.view.resetDevicePixelRatio();
      });

      await tester.pumpWidget(
        MaterialApp(
          home: MqttSettingsScreen(mqttService: testService),
        ),
      );
      await tester.pump();
      await tester.pump(const Duration(milliseconds: 100));

      final slugFinder = find.widgetWithText(TextFormField, 'Device Slug');
      expect(slugFinder, findsOneWidget);
      final slugFormField = tester.widget<TextFormField>(slugFinder);
      final initialSlug = slugFormField.controller!.text;
      expect(initialSlug, matches(defaultSlugPattern));

      // Auto-generated slug is persisted
      expect(await MqttSettings.getSlug(), equals(initialSlug));

      // Enable and modify slug
      await tester.tap(find.text('Enable Remote Control'));
      await tester.pump();
      await tester.pump(const Duration(milliseconds: 100));

      await tester.enterText(find.widgetWithText(TextFormField, 'Broker Host or IP'), '10.0.0.2');
      await tester.enterText(slugFinder, 'modified_tablet');
      await tester.pump();

      final saveBtn = find.widgetWithText(FilledButton, 'Save & Connect');
      await tester.ensureVisible(saveBtn);
      await tester.pump();
      await tester.pump(const Duration(milliseconds: 100));
      await tester.tap(saveBtn);
      await tester.pump();
      await tester.pump(const Duration(milliseconds: 100));
      await tester.pump(const Duration(seconds: 4));

      expect(await MqttSettings.getSlug(), equals('modified_tablet'));
    });
  });
}
