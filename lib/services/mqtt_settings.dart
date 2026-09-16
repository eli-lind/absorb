import 'dart:math';
import 'package:shared_preferences/shared_preferences.dart';

/// Manages persistence of MQTT remote control configuration across accounts.
/// All keys are global device-level preferences registered in ScopedPrefs._globalKeys.
class MqttSettings {
  static const keyEnabled = 'mqtt_enabled';
  static const keyHost = 'mqtt_host';
  static const keyPort = 'mqtt_port';
  static const keyUsername = 'mqtt_username';
  static const keyPassword = 'mqtt_password';
  static const keySlug = 'mqtt_slug';
  static const keyDiscoveryEnabled = 'mqtt_discovery_enabled';
  static const keyUseTls = 'mqtt_use_tls';

  static const defaultPort = 1883;
  static const defaultTlsPort = 8883;
  static const defaultDiscoveryEnabled = true;

  static const globalKeys = <String>{
    keyEnabled,
    keyHost,
    keyPort,
    keyUsername,
    keyPassword,
    keySlug,
    keyDiscoveryEnabled,
    keyUseTls,
  };

  static Future<bool> isEnabled() async {
    final prefs = await SharedPreferences.getInstance();
    return prefs.getBool(keyEnabled) ?? false;
  }

  static Future<void> setEnabled(bool value) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setBool(keyEnabled, value);
  }

  static Future<String> getHost() async {
    final prefs = await SharedPreferences.getInstance();
    return prefs.getString(keyHost) ?? '';
  }

  static Future<void> setHost(String value) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString(keyHost, value.trim());
  }

  static Future<int> getPort() async {
    final prefs = await SharedPreferences.getInstance();
    return prefs.getInt(keyPort) ?? defaultPort;
  }

  static Future<void> setPort(int value) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setInt(keyPort, value);
  }

  static Future<String> getUsername() async {
    final prefs = await SharedPreferences.getInstance();
    return prefs.getString(keyUsername) ?? '';
  }

  static Future<void> setUsername(String value) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString(keyUsername, value);
  }

  static Future<String> getPassword() async {
    final prefs = await SharedPreferences.getInstance();
    return prefs.getString(keyPassword) ?? '';
  }

  static Future<void> setPassword(String value) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString(keyPassword, value);
  }

  static Future<String>? _slugFuture;

  static Future<String> getSlug() async {
    if (_slugFuture != null) return _slugFuture!;
    return _slugFuture = _getOrGenerateSlug();
  }

  static Future<String> _getOrGenerateSlug() async {
    try {
      final prefs = await SharedPreferences.getInstance();
      final existing = prefs.getString(keySlug);
      if (existing != null && existing.isNotEmpty) {
        return existing;
      }
      final random = Random();
      final suffix = random.nextInt(0x10000).toRadixString(16).padLeft(4, '0');
      final generatedSlug = 'absorb_$suffix';
      await prefs.setString(keySlug, generatedSlug);
      return generatedSlug;
    } finally {
      _slugFuture = null;
    }
  }

  static Future<void> setSlug(String value) async {
    _slugFuture = null;
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString(keySlug, value.trim());
  }

  static Future<bool> isDiscoveryEnabled() async {
    final prefs = await SharedPreferences.getInstance();
    return prefs.getBool(keyDiscoveryEnabled) ?? defaultDiscoveryEnabled;
  }

  static Future<void> setDiscoveryEnabled(bool value) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setBool(keyDiscoveryEnabled, value);
  }

  static Future<bool> useTls() async {
    final prefs = await SharedPreferences.getInstance();
    return prefs.getBool(keyUseTls) ?? false;
  }

  static Future<void> setUseTls(bool value) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setBool(keyUseTls, value);
  }

  static Future<Map<String, dynamic>> toMap() async {
    return {
      'enabled': await isEnabled(),
      'host': await getHost(),
      'port': await getPort(),
      'username': await getUsername(),
      'password': await getPassword(),
      'slug': await getSlug(),
      'discoveryEnabled': await isDiscoveryEnabled(),
      'useTls': await useTls(),
    };
  }

  static Future<void> fromMap(Map<String, dynamic> map) async {
    if (map['enabled'] != null) await setEnabled(map['enabled'] as bool);
    if (map['host'] != null) await setHost(map['host'] as String);
    if (map['port'] != null) await setPort((map['port'] as num).toInt());
    if (map['username'] != null) await setUsername(map['username'] as String);
    if (map['password'] != null) await setPassword(map['password'] as String);
    if (map['slug'] != null) await setSlug(map['slug'] as String);
    if (map['discoveryEnabled'] != null) {
      await setDiscoveryEnabled(map['discoveryEnabled'] as bool);
    }
    if (map['useTls'] != null) await setUseTls(map['useTls'] as bool);
  }
}
