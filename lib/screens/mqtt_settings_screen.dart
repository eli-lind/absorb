import 'package:flutter/material.dart';
import '../services/mqtt_remote_service.dart';
import '../services/mqtt_settings.dart';
import '../widgets/overlay_toast.dart';

class MqttSettingsScreen extends StatefulWidget {
  final MqttRemoteService? mqttService;
  const MqttSettingsScreen({super.key, this.mqttService});

  @override
  State<MqttSettingsScreen> createState() => _MqttSettingsScreenState();
}

class _MqttSettingsScreenState extends State<MqttSettingsScreen> {
  final _formKey = GlobalKey<FormState>();
  final _hostController = TextEditingController();
  final _portController = TextEditingController();
  final _usernameController = TextEditingController();
  final _passwordController = TextEditingController();
  final _slugController = TextEditingController();

  bool _isLoading = true;
  bool _enabled = false;
  bool _useTls = false;
  bool _discoveryEnabled = true;
  bool _obscurePassword = true;

  late final MqttRemoteService _mqttService =
      widget.mqttService ?? MqttRemoteService();

  @override
  void initState() {
    super.initState();
    _mqttService.addListener(_onMqttStateChanged);
    _loadSettings();
  }

  @override
  void dispose() {
    _mqttService.removeListener(_onMqttStateChanged);
    _hostController.dispose();
    _portController.dispose();
    _usernameController.dispose();
    _passwordController.dispose();
    _slugController.dispose();
    super.dispose();
  }

  void _onMqttStateChanged() {
    if (mounted) setState(() {});
  }

  Future<void> _loadSettings() async {
    final enabled = await MqttSettings.isEnabled();
    final host = await MqttSettings.getHost();
    final port = await MqttSettings.getPort();
    final username = await MqttSettings.getUsername();
    final password = await MqttSettings.getPassword();
    final slug = await MqttSettings.getSlug();
    final discovery = await MqttSettings.isDiscoveryEnabled();
    final tls = await MqttSettings.useTls();

    if (!mounted) return;

    setState(() {
      _enabled = enabled;
      _hostController.text = host;
      _portController.text = port.toString();
      _usernameController.text = username;
      _passwordController.text = password;
      _slugController.text = slug;
      _discoveryEnabled = discovery;
      _useTls = tls;
      _isLoading = false;
    });
  }

  Future<void> _saveAndConnect() async {
    if (_enabled && !_formKey.currentState!.validate()) {
      return;
    }

    final port = int.tryParse(_portController.text.trim()) ??
        (_useTls ? MqttSettings.defaultTlsPort : MqttSettings.defaultPort);

    await MqttSettings.setEnabled(_enabled);
    await MqttSettings.setHost(_hostController.text.trim());
    await MqttSettings.setPort(port);
    await MqttSettings.setUsername(_usernameController.text.trim());
    await MqttSettings.setPassword(_passwordController.text);
    await MqttSettings.setSlug(_slugController.text.trim());
    await MqttSettings.setDiscoveryEnabled(_discoveryEnabled);
    await MqttSettings.setUseTls(_useTls);

    if (!mounted) return;

    if (_enabled) {
      showOverlayToast(context, 'Settings saved. Connecting...',
          icon: Icons.cloud_sync_rounded);
      await _mqttService.connectFromSettings();
    } else {
      _mqttService.disconnect();
      showOverlayToast(context, 'MQTT remote control disabled.',
          icon: Icons.cloud_off_rounded);
    }
  }

  Future<void> _disconnect() async {
    _mqttService.disconnect();
    if (mounted) {
      showOverlayToast(context, 'Disconnected.', icon: Icons.cloud_off_rounded);
    }
  }

  Widget _buildStatusPill(BuildContext context) {
    final cs = Theme.of(context).colorScheme;
    final status = _mqttService.connectionStatus;

    Color bg;
    Color border;
    Color fg;
    IconData icon;
    String label;
    bool spinning = false;

    switch (status) {
      case MqttConnectionStatus.connected:
        bg = Colors.green.withValues(alpha: 0.15);
        border = Colors.green;
        fg = Colors.green;
        icon = Icons.check_circle_rounded;
        label = 'Connected';
        break;
      case MqttConnectionStatus.connecting:
        bg = Colors.amber.withValues(alpha: 0.15);
        border = Colors.amber;
        fg = Colors.amber.shade800;
        icon = Icons.sync_rounded;
        label = 'Connecting...';
        spinning = true;
        break;
      case MqttConnectionStatus.error:
        bg = cs.errorContainer;
        border = cs.error;
        fg = cs.error;
        icon = Icons.error_outline_rounded;
        label = 'Connection Error';
        break;
      case MqttConnectionStatus.disconnected:
        bg = cs.surfaceContainerHigh;
        border = cs.outlineVariant;
        fg = cs.onSurfaceVariant;
        icon = Icons.cloud_off_rounded;
        label = _enabled ? 'Disconnected' : 'Disabled';
        break;
    }

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 8),
      decoration: BoxDecoration(
        color: bg,
        borderRadius: BorderRadius.circular(20),
        border: Border.all(color: border, width: 1.2),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          if (spinning)
            SizedBox(
              width: 16,
              height: 16,
              child: CircularProgressIndicator(
                strokeWidth: 2,
                color: fg,
              ),
            )
          else
            Icon(icon, size: 18, color: fg),
          const SizedBox(width: 8),
          Text(
            label,
            style: TextStyle(
              color: fg,
              fontWeight: FontWeight.w600,
              fontSize: 13,
            ),
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final cs = Theme.of(context).colorScheme;
    final tt = Theme.of(context).textTheme;

    if (_isLoading) {
      return Scaffold(
        appBar: AppBar(title: const Text('Remote Control (MQTT)')),
        body: const Center(child: CircularProgressIndicator()),
      );
    }

    return Scaffold(
      appBar: AppBar(
        title: const Text('Remote Control (MQTT)'),
      ),
      body: Form(
        key: _formKey,
        child: ListView(
          padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
          children: [
            Center(child: _buildStatusPill(context)),
            const SizedBox(height: 16),

            // Main enable card
            Card(
              elevation: 0,
              color: cs.surfaceContainerHigh,
              shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(14)),
              child: SwitchListTile(
                value: _enabled,
                onChanged: (val) => setState(() => _enabled = val),
                title: const Text('Enable Remote Control'),
                subtitle: Text(
                  'Control playback and sync state via MQTT broker',
                  style: tt.bodySmall?.copyWith(color: cs.onSurfaceVariant),
                ),
                secondary: Icon(Icons.settings_remote_rounded, color: cs.primary),
              ),
            ),

            const SizedBox(height: 16),

            // Connection settings card
            Card(
              elevation: 0,
              color: cs.surfaceContainerHigh,
              shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(14)),
              child: Padding(
                padding: const EdgeInsets.all(16),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Broker Configuration',
                      style: tt.titleSmall?.copyWith(
                        color: cs.primary,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    const SizedBox(height: 12),
                    TextFormField(
                      controller: _hostController,
                      enabled: _enabled,
                      decoration: const InputDecoration(
                        labelText: 'Broker Host or IP',
                        hintText: '192.168.1.50 or mqtt.home.local',
                        border: OutlineInputBorder(),
                        prefixIcon: Icon(Icons.dns_rounded),
                      ),
                      validator: (val) {
                        if (_enabled && (val == null || val.trim().isEmpty)) {
                          return 'Broker host is required';
                        }
                        return null;
                      },
                    ),
                    const SizedBox(height: 12),
                    Row(
                      children: [
                        Expanded(
                          flex: 3,
                          child: TextFormField(
                            controller: _portController,
                            enabled: _enabled,
                            keyboardType: TextInputType.number,
                            decoration: const InputDecoration(
                              labelText: 'Port',
                              hintText: '1883',
                              border: OutlineInputBorder(),
                              prefixIcon: Icon(Icons.numbers_rounded),
                            ),
                          ),
                        ),
                        const SizedBox(width: 12),
                        Expanded(
                          flex: 4,
                          child: SwitchListTile(
                            contentPadding: EdgeInsets.zero,
                            title: const Text('TLS', style: TextStyle(fontSize: 14)),
                            value: _useTls,
                            onChanged: _enabled
                                ? (val) {
                                    setState(() {
                                      _useTls = val;
                                      if (val &&
                                          _portController.text.trim() == '1883') {
                                        _portController.text = '8883';
                                      } else if (!val &&
                                          _portController.text.trim() == '8883') {
                                        _portController.text = '1883';
                                      }
                                    });
                                  }
                                : null,
                          ),
                        ),
                      ],
                    ),
                    const SizedBox(height: 12),
                    TextFormField(
                      controller: _usernameController,
                      enabled: _enabled,
                      decoration: const InputDecoration(
                        labelText: 'Username (optional)',
                        border: OutlineInputBorder(),
                        prefixIcon: Icon(Icons.person_outline_rounded),
                      ),
                    ),
                    const SizedBox(height: 12),
                    TextFormField(
                      controller: _passwordController,
                      enabled: _enabled,
                      obscureText: _obscurePassword,
                      decoration: InputDecoration(
                        labelText: 'Password (optional)',
                        border: const OutlineInputBorder(),
                        prefixIcon: const Icon(Icons.lock_outline_rounded),
                        suffixIcon: IconButton(
                          icon: Icon(_obscurePassword
                              ? Icons.visibility_outlined
                              : Icons.visibility_off_outlined),
                          onPressed: () => setState(
                              () => _obscurePassword = !_obscurePassword),
                        ),
                      ),
                    ),
                  ],
                ),
              ),
            ),

            const SizedBox(height: 16),

            // Device & Discovery card
            Card(
              elevation: 0,
              color: cs.surfaceContainerHigh,
              shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(14)),
              child: Padding(
                padding: const EdgeInsets.all(16),
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'Device Identification & Discovery',
                      style: tt.titleSmall?.copyWith(
                        color: cs.primary,
                        fontWeight: FontWeight.bold,
                      ),
                    ),
                    const SizedBox(height: 12),
                    TextFormField(
                      controller: _slugController,
                      enabled: _enabled,
                      decoration: const InputDecoration(
                        labelText: 'Device Slug',
                        hintText: 'kids_tablet',
                        helperText: 'Topic prefix: absorb/<slug>/...',
                        border: OutlineInputBorder(),
                        prefixIcon: Icon(Icons.devices_rounded),
                      ),
                    ),
                    const SizedBox(height: 8),
                    SwitchListTile(
                      contentPadding: EdgeInsets.zero,
                      value: _discoveryEnabled,
                      onChanged: _enabled
                          ? (val) => setState(() => _discoveryEnabled = val)
                          : null,
                      title: const Text('Home Assistant MQTT Discovery'),
                      subtitle: Text(
                        'Publish media player and sleep sensor discovery payloads',
                        style:
                            tt.bodySmall?.copyWith(color: cs.onSurfaceVariant),
                      ),
                    ),
                  ],
                ),
              ),
            ),

            const SizedBox(height: 24),

            // Actions
            FilledButton.icon(
              onPressed: _saveAndConnect,
              icon: const Icon(Icons.save_rounded),
              label: const Text('Save & Connect'),
              style: FilledButton.styleFrom(
                minimumSize: const Size.fromHeight(50),
                shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(12)),
              ),
            ),
            if (_mqttService.isConnected ||
                _mqttService.connectionStatus == MqttConnectionStatus.connecting) ...[
              const SizedBox(height: 12),
              OutlinedButton.icon(
                onPressed: _disconnect,
                icon: const Icon(Icons.power_settings_new_rounded),
                label: const Text('Disconnect'),
                style: OutlinedButton.styleFrom(
                  minimumSize: const Size.fromHeight(48),
                  shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(12)),
                ),
              ),
            ],
            const SizedBox(height: 24),
          ],
        ),
      ),
    );
  }
}
