# Absorb Remote Control

Context governing remote control of the Absorb Audiobookshelf client via MQTT for home automation and fleet management.

## Language

**MqttRemoteService**:
The singleton service in Absorb managing the MQTT broker lifecycle, state publication, and remote command dispatch.
_Avoid_: MQTT Bridge, Remote Controller, HA Client

**Home Assistant MQTT Discovery**:
The convention of publishing retained JSON configuration payloads under `homeassistant/<component>/...` to dynamically instantiate HA entities without manual YAML.
_Avoid_: Auto-discovery, manual config, HA Webhook

**Sleep Timer Entity**:
A discrete Home Assistant entity (sensor or button) reflecting Absorb's sleep countdown or triggering its end-of-chapter sleep action.
_Avoid_: Bedtime switch, timer helper

**Fleet Provisioning**:
The mechanism for distributing MQTT configuration across multiple tablets via `.absorb` backup/restore archives.
_Avoid_: Bulk enrollment, MDM push, build-time config

**Device Identifier Slug**:
A sanitized alphanumeric string used as the MQTT topic token and Home Assistant object ID.
_Avoid_: Device name, friendly name, client ID

**Throttled Heartbeat**:
The periodic 10-second position telemetry emitted during active playback to prevent radio churn.
_Avoid_: Polling rate, update frequency, stream tick
