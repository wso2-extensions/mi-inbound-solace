# Solace Inbound Endpoint for WSO2 Micro Integrator

The Solace inbound endpoint enables **WSO2 Integrator: MI** to consume messages from a [Solace PubSub+](https://solace.com/) event broker and inject them into a mediation sequence. It supports queues, direct topic subscriptions, and durable topic endpoints, with guaranteed-delivery acknowledgement, multiple authentication schemes, and TLS.

## Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Sample configurations](#sample-configurations)
- [Configuration reference](#configuration-reference)
- [Message properties](#message-properties)
- [Acknowledgement and failure handling](#acknowledgement-and-failure-handling)
- [Building from source](#building-from-source)
- [Documentation and license](#documentation-and-license)

## Features

- **Destination types** — `QUEUE` (guaranteed point-to-point), `TOPIC` with `DIRECT` subscriptions (non-persistent fanout, multiple comma-separated topics), and `TOPIC` with `DURABLE` subscriptions via a Durable Topic Endpoint (DTE).
- **Authentication** — `BASIC` (username/password), `CLIENT_CERTIFICATE` (mutual TLS), and `OAUTH2` / OIDC.
- **TLS** — custom trust store / key store, with optional certificate and certificate-date validation.
- **Guaranteed messaging** — client acknowledgement with configurable fallback settlement (`FAILED` → redeliver, `REJECTED` → DMQ), and a server-side SQL-92 message selector.
- **Destination provisioning** — auto-create the queue/DTE on the broker if it does not exist.
- **Rich metadata** — broker, header, QoS, and user-defined properties surfaced as synapse message-context properties.
- **Resilience** — automatic reconnect, and deadlock-safe teardown that lets the MI framework resume the listener after a terminal failure.

## Prerequisites

The Solace JCSMP client (`sol-jcsmp`) is **not bundled** with this inbound endpoint. Solace JCSMP is not Apache-2.0 licensed, so it cannot be redistributed or repackaged. You must install it once into the MI server's shared library directory, `<MI_HOME>/lib`, before deploying.

Installing it in `<MI_HOME>/lib` (rather than bundling it inside the inbound) is required so that this inbound endpoint and the [Solace connector](https://github.com/wso2-extensions/mi-connector-solace) share a **single** copy of the client. A single shared copy:

- avoids a Netty initialization clash — `java.lang.IllegalArgumentException: 'TOTAL_SOCKET_BYTES_SENT' is already in use` — that occurs when two copies of the JCSMP Netty transport are loaded in the same server (e.g. the inbound and the connector deployed together, or a hot-redeploy), and
- lets this inbound hand its received message to the connector's `acknowledge` / `nack` operations (both modules must resolve `com.solacesystems.jcsmp.BytesXMLMessage` from the same classloader).

### Install the required JARs

Place the following into `<MI_HOME>/lib` and restart the server:

| Artifact | Version |
|---|---|
| `com.solacesystems:sol-jcsmp` | `10.30.1` |
| `org.apache.servicemix.bundles:org.apache.servicemix.bundles.jzlib` | `1.1.3_2` |

Netty does **not** need to be added — the JCSMP client uses the Netty already shipped with MI (MI 4.6.0 bundles `netty-all 4.2.12`, which matches `sol-jcsmp 10.30.x`).

You can fetch both JARs from Maven into `lib` with:

```bash
mvn dependency:copy -Dartifact=com.solacesystems:sol-jcsmp:10.30.1 -DoutputDirectory="$MI_HOME/lib"
mvn dependency:copy -Dartifact=org.apache.servicemix.bundles:org.apache.servicemix.bundles.jzlib:1.1.3_2 -DoutputDirectory="$MI_HOME/lib"
```

Then restart WSO2 MI.

## Quick start

1. Install the required JARs into `<MI_HOME>/lib` (see [Prerequisites](#prerequisites)).
2. Download the inbound from the [WSO2 Connector Store](https://store.wso2.com/) (or build it — see [Building from source](#building-from-source)) and add it to your integration project.
3. Create an inbound endpoint and the sequence that will process the messages (see [Sample configurations](#sample-configurations)).
4. Deploy and start the MI server.

## Sample configurations

### Queue (guaranteed delivery)

```xml
<inboundEndpoint xmlns="http://ws.apache.org/ns/synapse"
                 name="solace_queue_listener"
                 sequence="solaceProcessSeq"
                 onError="solaceErrorSeq"
                 class="org.wso2.carbon.inbound.solace.SolaceEventListener"
                 suspend="false">
    <parameters>
        <parameter name="inbound.behavior">eventBased</parameter>
        <parameter name="sequential">true</parameter>
        <parameter name="coordination">true</parameter>
        <parameter name="solace.host">tcp://localhost:55555</parameter>
        <parameter name="solace.vpnName">default</parameter>
        <parameter name="solace.authenticationScheme">BASIC</parameter>
        <parameter name="solace.username">admin</parameter>
        <parameter name="solace.password">admin</parameter>
        <parameter name="solace.destinationType">QUEUE</parameter>
        <parameter name="solace.destinationName">orders.queue</parameter>
    </parameters>
</inboundEndpoint>
```

### Direct topic subscription (non-persistent)

```xml
<parameter name="solace.destinationType">TOPIC</parameter>
<parameter name="solace.subscriptionType">DIRECT</parameter>
<!-- comma-separated values subscribe to multiple topics -->
<parameter name="solace.destinationName">orders/new, orders/updated</parameter>
```

### Durable topic endpoint (persistent)

```xml
<parameter name="solace.destinationType">TOPIC</parameter>
<parameter name="solace.subscriptionType">DURABLE</parameter>
<parameter name="solace.destinationName">orders/&gt;</parameter>
<parameter name="solace.dteName">orders.dte</parameter>
```

### Manual acknowledgement

By default messages are auto-acknowledged on receipt. To control settlement from the mediation sequence (using the Solace connector's `acknowledgeMessage` / `nackMessage` operations), disable auto-ack. The listener then enforces sequential processing to avoid an ack-then-nack race.

```xml
<parameter name="solace.autoAck">false</parameter>
<!-- fallback used only if the sequence doesn't settle the message itself -->
<parameter name="solace.failureOutcome">REJECTED</parameter>
```

## Configuration reference

Parameter names map directly to inbound-endpoint `<parameter>` names. Required-ness is conditional on the chosen authentication scheme and destination type.

### Connection

| Parameter | Required | Default | Description |
|---|---|---|---|
| `solace.host` | Yes | `tcp://localhost:55555` | Broker host. Use `tcps://host:port` for TLS. |
| `solace.vpnName` | Yes | `default` | Message VPN name. |
| `solace.clientName` | No | _(broker-assigned)_ | Optional JCSMP session client name. |

### Authentication

| Parameter | Required | Default | Description |
|---|---|---|---|
| `solace.authenticationScheme` | Yes | `BASIC` | `BASIC`, `CLIENT_CERTIFICATE`, or `OAUTH2`. |
| `solace.username` | BASIC only | | Username for basic authentication. |
| `solace.password` | BASIC only | | Password. Supports Secure Vault expressions. |
| `solace.oauth2AccessToken` | OAUTH2 | | OAuth2 access token. Provide this or `solace.oidcIdToken`. |
| `solace.oidcIdToken` | OAUTH2 | | OIDC ID token (alternative to the access token). |
| `solace.oauth2IssuerIdentifier` | No | | OAuth2 issuer identifier. |
| `apiProvidedUsername` | No | | Only when the VPN has `allow-api-provided-username`. For `CLIENT_CERTIFICATE`, leave blank to use the cert Common Name; for `OAUTH2`, leave blank (identity comes from the token). |

### Destination

| Parameter | Required | Default | Description |
|---|---|---|---|
| `solace.destinationType` | Yes | `QUEUE` | `QUEUE` or `TOPIC`. |
| `solace.destinationName` | Yes | | Queue name, or topic string(s). For `TOPIC` + `DIRECT`, comma-separated values subscribe to multiple topics. |
| `solace.subscriptionType` | TOPIC | `DIRECT` | `DIRECT` (non-persistent) or `DURABLE` (persistent). |
| `solace.dteName` | TOPIC + DURABLE | | Durable Topic Endpoint name. |

### Consumer

| Parameter | Required | Default | Description |
|---|---|---|---|
| `solace.contentType` | No | _(auto-detect)_ | Override content type (`application/json`, `application/xml`, `text/plain`). Empty = detect from header/payload. |
| `solace.binaryPayloadAsBase64` | No | `false` | Base64-encode `BytesMessage` payloads instead of UTF-8 decoding (protects binary data). Auto-sets content type to `application/octet-stream`. |
| `solace.selector` | No | | Server-side SQL-92 selector on headers/user properties. Applies to `QUEUE` and DTE destinations. Example: `priority >= 5 AND tenant = 'acme'`. |

### Guaranteed messaging (QUEUE / durable TOPIC)

| Parameter | Required | Default | Description |
|---|---|---|---|
| `solace.autoAck` | No | `true` | Auto-acknowledge on receipt. Disable to settle from the sequence. |
| `solace.failureOutcome` | No | `NONE` | Fallback when the sequence doesn't settle. `NONE`: leave unsettled (broker redelivers). `FAILED`: redeliver immediately. `REJECTED`: route to DMQ. Ignored when `autoAck` is on. |
| `solace.subAckWindowSize` | No | `255` | In-flight guaranteed messages before an ack is required. |

### Connection retry

| Parameter | Default | Description |
|---|---|---|
| `solace.connectionTimeout` | `30000` | Initial connection timeout (ms). |
| `solace.connectRetries` | `3` | Initial connection retries. |
| `solace.connectRetriesPerHost` | `3` | Retries per host in the host list. |
| `solace.reconnectRetries` | `3` | Reconnect attempts after a connection loss. |
| `solace.reconnectRetryWait` | `3000` | Wait between reconnect attempts (ms). |

### Destination provisioning (when `solace.provisionDestination=true`)

| Parameter | Default | Description |
|---|---|---|
| `solace.provisionDestination` | `false` | Auto-create the queue/DTE if it doesn't exist. |
| `solace.queueAccessType` | `EXCLUSIVE` | `EXCLUSIVE` or `NON_EXCLUSIVE` (queues only). |
| `solace.queueQuotaMB` | `0` | Spool quota in MB. `0` = broker default. |
| `solace.queueMaxMsgSize` | `0` | Max message size in bytes. `0` = broker default. |
| `solace.queueRespectTTL` | `false` | Respect message TTL and discard expired messages. |
| `solace.queueMaxMsgRedelivery` | `0` | Max redelivery attempts before DMQ (queues only). `0` = broker default. |

### SSL/TLS

| Parameter | Default | Description |
|---|---|---|
| `solace.sslTrustStorePath` | _(JVM default)_ | Trust store (CAs) used to validate the broker certificate. |
| `solace.sslTrustStorePassword` | | Trust store password. |
| `solace.sslTrustStoreFormat` | `JKS` | `JKS` or `PKCS12`. |
| `solace.sslKeyStorePath` | | Client cert + key store. **Required** for `CLIENT_CERTIFICATE`. |
| `solace.sslKeyStorePassword` | | Key store password. |
| `solace.sslKeyStoreFormat` | `JKS` | `JKS` or `PKCS12`. |
| `solace.sslKeyPassword` | | Private-key entry password. |
| `solace.sslValidateCertificate` | `true` | Validate the broker certificate chain. **Disable only in dev/lab.** |
| `solace.sslValidateCertificateDate` | `true` | Validate the certificate's validity dates. |

### Advanced

| Parameter | Default | Description |
|---|---|---|
| `solace.generateReceiveTimestamps` | `false` | Record broker receive time, exposed as `solace.receiveTimestamp`. |

### Common inbound parameters

| Parameter | Default | Description |
|---|---|---|
| `sequential` | `true` | Process messages sequentially. Forced to `true` when `autoAck=false`. |
| `coordination` | `true` | In a cluster, only one node consumes at a time. |

## Message properties

Each injected message carries the following synapse message-context properties (set only when present on the message):

**Envelope:** `solace.messageId`, `solace.destination`, `solace.deliveryMode`, `solace.receiveTimestamp`, `solace.sequenceNumber`, `solace.expiration`, `solace.discardIndication`

**Guaranteed delivery only:** `solace.redelivered`, `solace.deliveryCount`

**Header (publisher-set):** `solace.priority`, `solace.correlationId`, `solace.replyTo`, `solace.senderId`, `solace.senderTimestamp`, `solace.applicationMessageId`, `solace.applicationMessageType`, `solace.timeToLive`

**HTTP bridging:** `solace.httpContentType`, `solace.httpContentEncoding`

**QoS / routing:** `solace.cos`, `solace.dmqEligible`, `solace.elidingEligible`

**User-defined:** each property in the Solace SDTMap is surfaced as `solace.userProp.<key>` (e.g. a property `region` becomes `solace.userProp.region`).

## Acknowledgement and failure handling

- With `solace.autoAck=true` (default), messages are acknowledged on receipt; the sequence cannot influence settlement.
- With `solace.autoAck=false`, settle the message from the sequence via the [Solace connector](https://github.com/wso2-extensions/mi-connector-solace)'s `acknowledgeMessage` / `nackMessage` operations. The listener pins `sequential=true` in this mode so settlement is always decided from a completed mediation, avoiding an ack-then-nack double-settle.
- `solace.failureOutcome` is the fallback used only when the sequence does not settle the message itself (e.g. an unhandled exception).
- Empty or unparseable payloads are settled as `REJECTED` (routed to the DMQ) so the message is preserved for inspection instead of being redelivered forever.

## Building from source

Requires JDK 11+ and Maven 3.6+.

```bash
mvn clean install
```

The connector bundle is produced at `target/mi-inbound-solace-<version>.zip`.

## Documentation and license

- Full documentation: [Solace connector overview](https://mi.docs.wso2.com/en/latest/reference/connectors/solace-connector/solace-connector-overview/)
- Companion connector (send/reply/ack/nack operations): [mi-connector-solace](https://github.com/wso2-extensions/mi-connector-solace)

Licensed under the [Apache License 2.0](LICENSE).
