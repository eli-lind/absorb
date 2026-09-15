# Native MQTT Client for Remote Control Control Plane

Absorb will connect directly to an external MQTT broker (such as Home Assistant's Mosquitto) as an MQTT client to provide bidirectional remote control and telemetry, rather than hosting an embedded HTTP/REST server or relaying through the upstream Audiobookshelf server.

## Considered options

### Embedded HTTP / REST server on the tablet
An HTTP server (e.g. Shelf) running inside the Absorb Android process listening for LAN REST calls.
**What would make this valid:** nothing; this is terminal. Android Doze and battery management terminate background TCP listeners, and inbound HTTP endpoints require NAT/firewall traversal on segmented kid VLANs without providing native availability (LWT) or push telemetry.

### Upstream Audiobookshelf server WebSocket relay
Adding a remote player dispatch API to the Audiobookshelf server to relay commands down through Absorb's existing socket.io connection.
**What would make this valid:** an upstream Audiobookshelf release shipping a native remote player control API that Home Assistant supports directly out of the box.

## Consequences

- Absorb requires broker connection settings (host, port, credentials, topic prefix).
- State telemetry survives while audio plays due to Android's foreground audio service keeping the Dart runtime active.
**Revisit condition:** once an upstream Audiobookshelf release provides native remote client control via the server API, re-evaluate whether direct client MQTT remains necessary.
