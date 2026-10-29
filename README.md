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
- [Broker side HA with coordination=false](#broker-side-ha-with-coordinationfalse)
- [Transacted consumption](#transacted-consumption)
- [OAuth2 token file rotation](#oauth2-token-file-rotation)
- [Health](#health)
- [2.0.0 breaking changes](#200-breaking-changes)
- [2.0.1 changes](#201-changes)
- [Building from source](#building-from-source)
- [Documentation and license](#documentation-and-license)

## Features

- **Destination types** — `QUEUE` (guaranteed point-to-point), `TOPIC` with `DIRECT` subscriptions (non-persistent fanout, multiple comma-separated topics), and `TOPIC` with `DURABLE` subscriptions via a Durable Topic Endpoint (DTE).
- **Authentication** — `BASIC` (username/password), `CLIENT_CERTIFICATE` (mutual TLS), and `OAUTH2` / OIDC.
- **TLS** — custom trust store / key store, with optional certificate and certificate-date validation.
- **Guaranteed messaging** — client acknowledgement with configurable fallback settlement (`FAILED` → redeliver, `REJECTED` → DMQ), and a server-side SQL-92 message selector.
- **Destination provisioning** — auto-create the queue/DTE on the broker if it does not exist.
- **Rich metadata** — broker, header, QoS, and user-defined properties surfaced as synapse message-context properties.
- **Resilience** — automatic reconnect, and recovery inside the inbound. A failed generation is rebuilt with a back off, and the endpoint never depends on the framework to resume it.

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

By default messages are acknowledged when the injection returns, before any asynchronous work of the sequence completes, at most once. To control settlement from the mediation sequence (using the Solace connector's `acknowledgeMessage` / `nackMessage` operations), disable auto-ack. The listener then enforces sequential processing to avoid an ack-then-nack race.

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
| `solace.clientName` | No | empty | Client name prefix. The endpoint appends the node and the endpoint name unless `solace.clientNameStrict` is true. Empty gives `<node>-<endpointName>`. |
| `solace.clientNameStrict` | No | `false` | When `false`, the session name is `solace.clientName`, if set, followed by the node id and the endpoint name, so that a rebuilt session evicts its old one. A name longer than 160 bytes is cut and ends with a short hash. The node id is the `node.id` system property, else the `nodeId` system property or environment variable, else `[cluster_config] node_id` in deployment.toml, and only then the host name. The inbound logs at INFO which source it used. Two instances on one host need distinct node ids. When `true`, `solace.clientName` is sent unchanged, as in 1.x. |
| `solace.compressionLevel` | No | `0` | Compression level of the connection, from `0` to `9`. `0` turns compression off. |

### Authentication

| Parameter | Required | Default | Description |
|---|---|---|---|
| `solace.authenticationScheme` | Yes | `BASIC` | `BASIC`, `CLIENT_CERTIFICATE`, or `OAUTH2`. |
| `solace.username` | BASIC only | | Username for basic authentication. |
| `solace.password` | BASIC only | | Password. Supports Secure Vault expressions. |
| `solace.oauth2AccessToken` | OAUTH2 | | OAuth2 access token. Provide this, `solace.oidcIdToken` or `solace.oauth2TokenFile`. |
| `solace.oidcIdToken` | OAUTH2 | | OIDC ID token (alternative to the access token). |
| `solace.oauth2IssuerIdentifier` | No | | OAuth2 issuer identifier. |
| `solace.apiProvidedUsername` | No | | Only when the VPN has `allow-api-provided-username`. For `CLIENT_CERTIFICATE`, leave blank to use the cert Common Name; for `OAUTH2`, leave blank (identity comes from the token). The old name `apiProvidedUsername` is still accepted. |
| `solace.oauth2TokenFile` | OAUTH2 | | File the OAuth2 access token is read from at every connect and at every reconnect attempt, so that a sidecar or an operator can replace it. A missing or unreadable file keeps the last token in use and logs a WARN. Requires `solace.authenticationScheme=OAUTH2`. |

### Destination

| Parameter | Required | Default | Description |
|---|---|---|---|
| `solace.destinationType` | Yes | `QUEUE` | `QUEUE` or `TOPIC`. |
| `solace.destinationName` | Yes | | Queue name, or topic string(s). For `TOPIC` + `DIRECT`, comma-separated values subscribe to multiple topics. |
| `solace.subscriptionType` | TOPIC | `DIRECT` | `DIRECT` (non-persistent) or `DURABLE` (persistent). |
| `solace.dteName` | TOPIC + DURABLE | | Durable Topic Endpoint name. |
| `solace.sharedSubscriptionGroup` | No | | For a Direct topic with `coordination=false`, subscribes `#share/<group>/<topic>` so that each message reaches one node. Without it every node receives every message. |

### Consumer

| Parameter | Required | Default | Description |
|---|---|---|---|
| `solace.contentType` | No | | The content type the body is read with. It wins over the type the message declares. Empty or blank uses the declared type, else the default of the message class: `text/plain` for a TextMessage, `application/octet-stream` for a BytesMessage, `application/xml` for an XMLContentMessage and `application/json` for a MapMessage or a StreamMessage. A value without a charset takes the charset the message declares, else UTF-8. The payload is never inspected to guess a type. |
| `solace.binaryPayloadAsBase64` | No | `false` | `false`: the body is built by the builder registered for its content type. A text type is decoded with its charset, and a binary type gives the native binary element, so the sequence can send the bytes over HTTP or write them with the File connector. `true` is the legacy option: a BytesMessage body is base64 text with `application/octet-stream`, as in 2.0.0, and the sequence decodes it. |
| `solace.selector` | No | | Server-side SQL-92 selector on headers/user properties. Applies to `QUEUE` and DTE destinations. Example: `priority >= 5 AND tenant = 'acme'`. |

### Guaranteed messaging (QUEUE / durable TOPIC)

| Parameter | Required | Default | Description |
|---|---|---|---|
| `solace.autoAck` | No | `true` | Acknowledged when the injection returns, at most once. Disable to settle from the sequence. |
| `solace.failureOutcome` | No | `FAILED` | Outcome applied when no settlement arrives within `solace.settlementTimeout`. `FAILED`: retry in place as `solace.maxRedeliveries` allows. `REJECTED`: route to the DMQ. `NONE`: leave unsettled, counted as `retainedUnsettled`. Ignored when `autoAck` is on. |
| `solace.subAckWindowSize` | No | `255` | Transport window, 1 to 255. The broker side in flight bound together with `concurrentConsumers` and the pending queue, and the high water mark of the flow control. |
| `solace.settlementTimeout` | No | `180000` | Time in ms the sequence has to settle a message. When the parameter is absent, `[messaging_callback] solace_inbound_settlement_timeout_ms` in deployment.toml applies, and then the default. It must exceed the longest endpoint or blocking timeout in the sequence plus the 15 s sweep of the timeout handler, so that the fault sequence decides before the inbound does. On a stock pack the passthrough socket timeout is also 180 s, so an endpoint whose backend can hang sets the HTTP connector's `timeoutDuration` below 165 s or raises this value. |
| `solace.concurrentConsumers` | No | `1` | Number of workers, which is also the bound on delivery attempts in progress. `1` keeps the order of delivery attempts strict together with the in place retry, except for continuations of earlier attempts that still run. `solace.maxRedeliveries=0` brings back the reorder of a broker redelivery. |
| `solace.maxRedeliveries` | No | `-1` | In place retries of a failed message. `-1`: retry without limit. `N`: retry `N` times, then `REJECTED`. `0`: no retry in the inbound, settle `FAILED` at once and let the broker redelivery settings apply. Ignored in transacted mode, with a WARN at start. |
| `solace.redelivery.initialDelay` / `.progression` / `.maxDelay` | No | `1000` / `2.0` / `60000` | The delay between in place retries starts at `initialDelay` ms, grows by `progression` after each attempt and stops growing at `maxDelay` ms. In transacted mode it applies before the rollback, with the attempt taken from the broker's delivery count. |
| `solace.requireSettlementOutcomes` | No | `AUTO` | `AUTO` requests the settlement outcomes the broker reports. A message that needs a missing outcome is kept unsettled with a WARN, and the inbound reconnects so that the broker redelivers it. `TRUE` fails the bind as 1.x did. |
| `solace.transacted` | No | `false` | Consume each message in its own local transaction, committed on `ACK` and rolled back otherwise. Refused with `autoAck=true` and on any topic. |
| `solace.drainTimeout` | No | `30000` | Upper limit in ms of a pause, a resume and an undeploy. At least `3000`. The last 2000 ms are the budget to settle the messages still in flight. |
| `solace.drainOutcome` | No | `NONE` | Outcome for messages still in flight when the drain reaches its settle budget. `NONE`: leave them to broker redelivery. `REJECTED`: route them to the DMQ. |
| `solace.queuePermission` | No | empty | Permission the endpoint sets on a queue it provisions: `CONSUME`, `DELETE`, `MODIFY_TOPIC` or `READ_ONLY`. Empty or any other value leaves the broker default. |

### Connection retry

| Parameter | Default | Description |
|---|---|---|
| `solace.connectionTimeout` | `30000` | Initial connection timeout (ms). |
| `solace.connectRetries` | `3` | Initial connection retries. |
| `solace.connectRetriesPerHost` | `3` | Retries per host in the host list. |
| `solace.reconnectRetries` | `-1` | Reconnect attempts after a connection loss. `-1` retries without limit. |
| `solace.reconnectRetryWait` | `3000` | Wait between reconnect attempts (ms). |
| `solace.recovery.initialDelay` / `.progression` / `.maxDelay` | `1000` / `2.0` / `60000` | The inbound runs its own recovery loop when a session fails. The delay between its connection attempts starts at `initialDelay` ms, grows by `progression` after each failed attempt and stops growing at `maxDelay` ms. |
| `solace.recovery.maxAttempts` | `-1` | Connection attempts before the endpoint stops in `FAILED`. `-1` keeps trying without limit. |
| `solace.keepAliveInterval` | _(JCSMP default)_ | Interval in ms between keep alive messages, at least 50. Empty keeps the JCSMP default. |
| `solace.keepAliveLimit` | _(JCSMP default)_ | Keep alive messages without a reply before the connection is closed, at least 1. Empty keeps the JCSMP default. |

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
| `solace.sslValidateHostname` | `true` | Check that the broker certificate was issued for the host the inbound connects to. |

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

**Guaranteed delivery only:** `solace.deliveryCount`

**Header (publisher-set):** `solace.priority`, `solace.correlationId`, `solace.replyTo`, `solace.senderId`, `solace.senderTimestamp`, `solace.applicationMessageId`, `solace.applicationMessageType`, `solace.timeToLive`

**HTTP bridging:** `solace.httpContentType`, `solace.httpContentEncoding`

**QoS / routing:** `solace.cos`, `solace.dmqEligible`, `solace.elidingEligible`

**User-defined:** each property in the Solace SDTMap is surfaced as `solace.userProp.<key>` (e.g. a property `region` becomes `solace.userProp.region`). A byte value is base64 text.

2.0.0 adds or changes these properties on every message.

- `solace.deliveryAttempt`: the number of this in place attempt, `1` on the first injection.
- `solace.redelivered`: `true` when the broker's redelivered flag is set, and also from the second in place attempt. It is now set on Direct topic messages too.
- `solace.inbound.settlement`: the settlement handle that the connector's `acknowledgeMessage` and `nackMessage` operations complete. Do not set or remove it.

2.0.0 sets these properties only in transacted mode, `solace.transacted=true`.

- `solace.inbound.tx`: the command executor of the transacted worker. Connector operations inside the sequence submit their broker work to it.
- `solace.inbound.tx.producer`: the producer of the transacted worker. `publishMessage` and `sendReply` publish on it, inside the transaction.

## Acknowledgement and failure handling

- With `solace.autoAck=true` (default), messages are acknowledged when the injection returns, before any asynchronous work of the sequence completes, at most once. The sequence cannot influence settlement.
- With `solace.autoAck=false`, settle the message from the sequence via the [Solace connector](https://github.com/wso2-extensions/mi-connector-solace)'s `acknowledgeMessage` / `nackMessage` operations. The listener pins `sequential=true` in this mode so settlement is always decided from a completed mediation, avoiding an ack-then-nack double-settle.
- `solace.failureOutcome` is the fallback used only when the sequence does not settle the message itself (e.g. an unhandled exception).
- A message with an empty body builds no mediation context and never reaches the sequence, so a header only message is not processed. The endpoint logs `Received empty Solace message, no mediation context built` and the settlement table decides as for a rejected injection: acknowledged with `solace.autoAck=true`, settled `REJECTED` with `solace.autoAck=false`, and a DMQ eligible message goes to the DMQ of the queue, and rolled back in transacted mode.
- A body that cannot be built takes the same path with the line `could not be converted into a message context; rejecting`. That is text that does not decode with its charset, an XML document with a malformed root, or JSON the builder rejects at its start. A JSON or XML document that is damaged after its start is injected and fails where the sequence first reads the body, and the settlement table decides as for any failed sequence. We do not promise that every read of such a body throws. A sequence that needs a validated payload parses and validates it explicitly. A sequence that acknowledges without reading the body has not validated it.

## Broker side HA with coordination=false

The HA mode is the generic inbound parameter `coordination`. There is no Solace specific HA parameter. With `coordination=true`, the default and the 1.x behaviour, the endpoint runs as a coordinated task and one node of the cluster binds. When that node dies, the coordination heartbeat detects it after the heartbeat interval times the maximum retries, 15 s with the defaults. A surviving node then connects and binds. The broker redelivers the messages that were in flight on the dead node once it drops that node's session.

With `coordination=false` every node binds its own flow and the broker decides which node consumes. On an exclusive queue and on a durable topic endpoint the flows bind with active flow indication when the broker reports the `ACTIVE_FLOW_INDICATION` capability. The consuming node logs `flow ACTIVE` and reports `active`. The other nodes report `unknown` from the bind until a `FLOW_INACTIVE` or a `FLOW_ACTIVE` event reaches them, because the broker sends no event to a flow that binds as the standby. A node whose flow was active and lost that role logs `flow INACTIVE` and reports `standby`. A standby flow is already bound, so failover needs only the broker's detection of the lost session. Without the capability the flows still bind and the broker still fails over. The inbound then logs a WARN at bind, and every node reports activity `unknown`. On a non exclusive queue all nodes consume, and the order of messages across nodes is not kept, the same contract as `solace.concurrentConsumers` above 1. On a Direct topic every node receives every message by default, as in 1.x, and the inbound logs one INFO line at bind that says so. Set `solace.sharedSubscriptionGroup=<name>` to subscribe `#share/<name>/<topic>`, so that the broker spreads the stream across the nodes of the group. When the parameter is set and the broker does not report `SHARED_SUBSCRIPTIONS`, the endpoint stops in `FAILED` with a message that names the parameter. The client name carries the node id, so each node has its own session name unless `solace.clientNameStrict=true`. Each node runs its own recovery and drain, and the management API reports `active`, `standby` or `connected` per node. Health never triggers a recovery.

## Transacted consumption

With `solace.transacted=true` each worker owns one transacted session, one flow on it that it reads synchronously, and one producer. The worker thread is the only thread that touches the three. A connector operation inside the sequence, on any thread, submits its broker work to the worker through `solace.inbound.tx` and waits for the result. When the caller is the worker thread itself, the work runs inline. `publishMessage` and `sendReply` publish on the worker's producer, inside the transaction. The worker commits when the sequence calls `acknowledgeMessage`. A `nackMessage` with either outcome, the settlement timeout and a message that cannot be converted into a message context roll the transaction back, and the broker redelivers the message. When an operation stops waiting while its work still runs, it reports `outcome=UNKNOWN`, and the worker rolls the transaction back even after an acknowledgement.

Each transaction holds one message. The worker receives the next message only after the commit or the rollback of the current one. The settlement timeout, the redelivery delays and the drain apply as in the other modes, and the attempt number is the broker's delivery count. `solace.concurrentConsumers` workers bind that many transacted flows. On an exclusive queue only one of them is active, so the concurrency there is one. When the result of a commit is unknown after a connection failure, the worker rebuilds its session, flow and producer. The message may then be redelivered or gone, so this mode is at least once.

A message that always fails rolls back again and again. A transacted rollback returns every message the broker has delivered into the flow's window, received by the application or not, and counts a redelivery for each of them. With the queue attribute `maxDeliveredUnackedMsgsPerFlow` at 1 the broker holds the next message until the transaction completes, so the messages behind a rolling back message never enter its transaction and never count. A transacted endpoint therefore requires three settings on its queue, the maximum redelivery count, a DMQ, and max delivered unacknowledged messages per flow at 1. `solace.maxRedeliveries` is ignored in this mode, and the client window `solace.subAckWindowSize` does not replace the queue attribute. JCSMP cannot read or set the third setting, so the endpoint cannot verify the queue settings through JCSMP, and provisioning with "ignore already exists" does not change an existing queue. It logs a WARN at start, and the settings must be confirmed on the broker.

Some connector operations fault inside an inbound transaction. `poll`, `beginTransaction`, `commit` and `rollback` fault with `INBOUND_TRANSACTION` (700113). The inbound worker owns the transaction and decides its commit or rollback from the settlement, and the bridge to the connector carries the producer, not the session. `sendRequest` faults with `REQUEST_IN_TRANSACTION` (700114) in any transaction, because a transacted publish stays invisible to the responder until the commit, so the reply could never arrive before it.

A publish that runs inline on the worker thread has no `ackTimeout`. It returns when the publisher window of JCSMP admits it or its reconnect gives up. With the default `solace.reconnectRetries=-1` it can wait as long as the broker is unreachable. We accept this limit, because the worker cannot time itself out.

## OAuth2 token file rotation

With `solace.oauth2TokenFile` the inbound reads the access token from the file at every connect and at every reconnect attempt. When the session starts to reconnect, the inbound sets the token from the file on the session. The file is read whole and trimmed, so a file that an operator or a sidecar replaces atomically is always seen complete. A missing or unreadable file keeps the last token in use and logs one WARN per failure streak, and one INFO when the file is readable again. The token value never appears in a log line. A sidecar or an operator keeps the file current, which is the usual Kubernetes pattern. The static `solace.oauth2AccessToken` remains for compatibility.

## Health

On a WSO2 Integrator: MI level whose management API reads consumer health, the inbound endpoint resource carries a `health` object and takes its `status` from the endpoint. The health has three axes.

- `lifecycle` is the state of the endpoint, one of `stopped`, `connecting`, `running`, `draining`, `recovering` and `failed`.
- `connectivity` is kept per resource, which is the session, each flow and, for a Direct topic, the subscriptions. It comes from the reconnect events of JCSMP, which change no lifecycle state. The subscriptions of a Direct topic stay `reconnecting` until the session has reconnected and restored them. The aggregate is `disconnected` while no resource is known, which is before the first bind of a generation and after a refusal at start. Otherwise it is `connected` only when every resource is connected and `reconnecting` when any resource is not, so one flow that reconnects cannot hide another flow that is still down. Each resource is also shown on its own, for example `connectivity.session` and `connectivity.flow-1`.
- `activity` is kept per flow and is `active`, `standby` or `unknown`. A flow is `unknown` until its first `FLOW_ACTIVE` or `FLOW_INACTIVE`, and always when the broker lacks active flow indication. The flows request active flow indication in both coordination modes, so with `coordination=true` the one bound flow reads `active` on an exclusive queue once its `FLOW_ACTIVE` event arrives. The aggregate is `active` when any flow is active, `standby` when every flow is known and standby, and `unknown` otherwise. Each flow is also shown on its own, for example `activity.flow-1`.

`status` is the one word the management API shows. It is `failed` for a failed endpoint and `stopped` for a stopped one. It is `recovering` while the lifecycle is `connecting`, `draining` or `recovering`, or while the connectivity is `reconnecting`. Otherwise it is `active`, `standby`, or `connected` when the activity is unknown. A healthy endpoint shows `active` once the event arrives, in either coordination mode, and `connected` before the event or on a broker without active flow indication. Each connection attempt is a generation, reported as `generation`, and events from a retired generation are ignored. Health never triggers a recovery. The axes, `status` and `generation` are read under one lock, so they come from one point in time. The counters beside them, such as `inFlight`, `received` and `decided.ACK`, are written by the workers without that lock, so they are an approximate reading taken during the snapshot. `dropped` counts the Direct messages the pending queue could not hold and the parked messages discarded when their flow left the connected state. The broker redelivers the parked ones after the rebind, so a rising `dropped` on a flow loss is not a loss of messages.

## 2.0.0 breaking changes

- `failureOutcome` default `NONE` → `FAILED`.
- Manual ack sequences that relied on the implicit ack on return now time out and apply `failureOutcome` unless they call `acknowledgeMessage`. There is no compatibility switch. The deploy time WARN names endpoints whose sequences contain no settlement operation.
- Inbound 2.0.0 requires connector 2.0.0 in every mode and refuses to start with a 1.x connector deployed.
- `reconnectRetries` default `3` → `-1`.
- `publishMessage`, `sendRequest` and `sendReply` wait for the broker ack by default on guaranteed delivery, under the `ackTimeout` deadline.
- `solace.clientName` is a prefix unless `clientNameStrict=true`. The client name is never used for authorization on the broker, so this changes only what operators see in monitoring, the SEMP disconnect action, and any `#P2P` addressing by client name. People who depend on the exact string set `clientNameStrict=true`.
- Replies and polls no longer use pooled sessions. The connector opens up to two extra sessions per logical connection, so the broker's per client username connection limit must allow pool size plus two.

## 2.0.1 changes

- The content type of a message is `solace.contentType`, else the type the message declares, else the default of the message class. 1.x and 2.0.0 guessed JSON from `{` and `[` and XML from `<` at the start of an undeclared body. 2.0.1 does not. A producer declares the type, or the endpoint sets `solace.contentType`. No option restores the guess.
- The body is built by the builder registered for its content type. A text body is decoded with its charset and a binary BytesMessage keeps its bytes. 2.0.0 decoded every BytesMessage as UTF-8.
- Declared JSON or XML that the builder rejects is rejected with the build failure line. 1.x and 2.0.0 let it fall back to text. A JSON or XML document that is damaged after its start fails where the sequence first reads it.
- A byte value in the user properties is base64 text in `solace.userProp.<key>`.
- The empty message WARN reads `Received empty Solace message, no mediation context built` in every mode.

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
