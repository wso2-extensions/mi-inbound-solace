/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.inbound.solace;

/**
 * Constants used by the Solace Inbound Endpoint.
 */
public final class SolaceInboundConstants {

    private SolaceInboundConstants() {
        // Utility class
    }

    // Connection parameters (inbound endpoint XML parameter names)
    public static final String HOST        = "solace.host";
    public static final String VPN_NAME    = "solace.vpnName";
    public static final String USERNAME    = "solace.username";
    public static final String API_PROVIDED_USERNAME = "apiProvidedUsername";
    public static final String PASSWORD    = "solace.password";
    public static final String CLIENT_NAME = "solace.clientName";

    // Authentication scheme parameters
    /** Authentication scheme: BASIC | CLIENT_CERTIFICATE | OAUTH2 */
    public static final String AUTH_SCHEME                    = "solace.authenticationScheme";
    public static final String AUTH_SCHEME_BASIC              = "BASIC";
    public static final String AUTH_SCHEME_CLIENT_CERTIFICATE = "CLIENT_CERTIFICATE";
    public static final String AUTH_SCHEME_OAUTH2             = "OAUTH2";

    /** OAuth2 access token (used when authenticationScheme=OAUTH2) */
    public static final String OAUTH2_ACCESS_TOKEN      = "solace.oauth2AccessToken";
    /** OAuth2 issuer identifier (optional) */
    public static final String OAUTH2_ISSUER_IDENTIFIER = "solace.oauth2IssuerIdentifier";
    /** OIDC ID token (alternative to access token) */
    public static final String OIDC_ID_TOKEN            = "solace.oidcIdToken";

    // Destination / subscription parameters
    /** QUEUE or TOPIC */
    public static final String DESTINATION_TYPE          = "solace.destinationType";
    /** Queue name or topic string(s). For TOPIC + DIRECT, a comma-separated list is accepted. */
    public static final String DESTINATION_NAME          = "solace.destinationName";
    /** Required for topic subscriptions. DIRECT or DURABLE. */
    public static final String SUBSCRIPTION_TYPE          = "solace.subscriptionType";
    public static final String SUBSCRIPTION_TYPE_DIRECT   = "DIRECT";
    public static final String SUBSCRIPTION_TYPE_DURABLE  = "DURABLE";
    /** Optional name for durable subscription. Required if subscriptionType is DURABLE. */
    public static final String DTE_NAME                   = "solace.dteName";

    // Destination type values
    public static final String DESTINATION_TYPE_QUEUE         = "QUEUE";
    public static final String DESTINATION_TYPE_TOPIC         = "TOPIC";

    // Consumer parameters
    public static final String AUTO_ACK              = "solace.autoAck";
    public static final String SUB_ACK_WINDOW_SIZE   = "solace.subAckWindowSize";
    /** On injection failure: NONE (skip ack, let redelivery limit decide),
     *  FAILED (settle as FAILED → redeliver), REJECTED (settle as REJECTED → DMQ). */
    public static final String FAILURE_OUTCOME            = "solace.failureOutcome";
    public static final String FAILURE_OUTCOME_NONE       = "NONE";
    public static final String FAILURE_OUTCOME_FAILED     = "FAILED";
    public static final String FAILURE_OUTCOME_REJECTED   = "REJECTED";
    /** When true, BytesMessage payloads are base64-encoded rather than UTF-8 decoded. */
    public static final String BINARY_PAYLOAD_AS_BASE64 = "solace.binaryPayloadAsBase64";
    /** SQL-92 selector expression applied server-side on queue/DTE flows. */
    public static final String SELECTOR = "solace.selector";

    // Connection retry parameters
    public static final String CONNECTION_TIMEOUT       = "solace.connectionTimeout";
    public static final String CONNECT_RETRIES          = "solace.connectRetries";
    public static final String CONNECT_RETRIES_PER_HOST = "solace.connectRetriesPerHost";
    public static final String RECONNECT_RETRIES        = "solace.reconnectRetries";
    public static final String RECONNECT_RETRY_WAIT     = "solace.reconnectRetryWait";

    // SSL/TLS parameters
    public static final String SSL_TRUST_STORE_PATH          = "solace.sslTrustStorePath";
    public static final String SSL_TRUST_STORE_PASSWORD      = "solace.sslTrustStorePassword";
    public static final String SSL_TRUST_STORE_FORMAT        = "solace.sslTrustStoreFormat";
    public static final String SSL_KEY_STORE_PATH            = "solace.sslKeyStorePath";
    public static final String SSL_KEY_STORE_PASSWORD        = "solace.sslKeyStorePassword";
    public static final String SSL_KEY_STORE_FORMAT          = "solace.sslKeyStoreFormat";
    public static final String SSL_KEY_PASSWORD              = "solace.sslKeyPassword";
    public static final String SSL_VALIDATE_CERTIFICATE      = "solace.sslValidateCertificate";
    public static final String SSL_VALIDATE_CERTIFICATE_DATE = "solace.sslValidateCertificateDate";

    /** Enables JCSMP receive-time stamping on incoming messages. */
    public static final String GENERATE_RECEIVE_TIMESTAMPS = "solace.generateReceiveTimestamps";

    // Queue provisioning parameters (used only when solace.provisionDestination=true)
    public static final String PROVISION_DESTINATION    = "solace.provisionDestination";
    public static final String QUEUE_ACCESS_TYPE        = "solace.queueAccessType";
    public static final String QUEUE_ACCESS_EXCLUSIVE   = "EXCLUSIVE";
    public static final String QUEUE_ACCESS_NON_EXCLUSIVE = "NON_EXCLUSIVE";
    public static final String QUEUE_PERMISSION         = "solace.queuePermission";
    public static final String QUEUE_QUOTA_MB           = "solace.queueQuotaMB";
    public static final String QUEUE_MAX_MSG_SIZE       = "solace.queueMaxMsgSize";
    public static final String QUEUE_RESPECT_TTL        = "solace.queueRespectTTL";
    public static final String QUEUE_MAX_MSG_REDELIVERY = "solace.queueMaxMsgRedelivery";

    // Content type values
    public static final String CONTENT_TYPE = "solace.contentType";
    public static final String CONTENT_TYPE_JSON     = "application/json";
    public static final String CONTENT_TYPE_XML      = "application/xml";
    public static final String CONTENT_TYPE_TEXT_XML = "text/xml";
    public static final String CONTENT_TYPE_TEXT     = "text/plain";
    public static final String CONTENT_TYPE_OCTET    = "application/octet-stream";
    public static final String DEFAULT_CONTENT_TYPE  = CONTENT_TYPE_JSON;

    // Synapse MessageContext properties - Set by SolaceInjectHandler on every inbound message context.
    // --- Envelope (broker-set) ---
    public static final String SOLACE_MESSAGE_ID         = "solace.messageId";
    public static final String SOLACE_DESTINATION        = "solace.destination";
    public static final String SOLACE_DELIVERY_MODE      = "solace.deliveryMode";
    public static final String SOLACE_RECEIVE_TIMESTAMP  = "solace.receiveTimestamp";
    public static final String SOLACE_SEQUENCE_NUMBER    = "solace.sequenceNumber";
    public static final String SOLACE_EXPIRATION         = "solace.expiration";
    public static final String SOLACE_DISCARD_INDICATION = "solace.discardIndication";

    // --- Guaranteed delivery only (PERSISTENT / NON_PERSISTENT) ---
    public static final String SOLACE_REDELIVERED    = "solace.redelivered";
    public static final String SOLACE_DELIVERY_COUNT = "solace.deliveryCount";

    // --- Header (publisher-set) ---
    public static final String SOLACE_PRIORITY                 = "solace.priority";
    public static final String SOLACE_CORRELATION_ID           = "solace.correlationId";
    public static final String SOLACE_REPLY_TO                 = "solace.replyTo";
    public static final String SOLACE_SENDER_ID                = "solace.senderId";
    public static final String SOLACE_SENDER_TIMESTAMP         = "solace.senderTimestamp";
    public static final String SOLACE_APPLICATION_MESSAGE_ID   = "solace.applicationMessageId";
    public static final String SOLACE_APPLICATION_MESSAGE_TYPE = "solace.applicationMessageType";
    public static final String SOLACE_TIME_TO_LIVE             = "solace.timeToLive";

    // --- HTTP bridging headers ---
    public static final String SOLACE_HTTP_CONTENT_TYPE     = "solace.httpContentType";
    public static final String SOLACE_HTTP_CONTENT_ENCODING = "solace.httpContentEncoding";

    // --- QoS / routing ---
    public static final String SOLACE_COS             = "solace.cos";
    public static final String SOLACE_DMQ_ELIGIBLE    = "solace.dmqEligible";
    public static final String SOLACE_ELIDING_ELIGIBLE = "solace.elidingEligible";

    /**
     * The raw inbound {@code BytesXMLMessage} stashed on the MessageContext so
     * downstream connector operations (sendReply, acknowledgeMessage, nackMessage)
     * can access the original message handle. Must match
     * {@code SolaceConstants.SOLACE_INBOUND_MESSAGE} in the connector module.
     */
    public static final String SOLACE_INBOUND_MESSAGE = "solace.inbound.message";

    /**
     * Set to {@code Boolean.TRUE} by acknowledgeMessage / nackMessage after they settle the
     * inbound JCSMP message. The listener checks this after mediation and skips its own
     * post-mediation ack/nack so the broker doesn't see a conflicting second settlement
     * (which corrupts redelivery counting and can cause infinite redelivery). Must match
     * {@code SolaceConstants.SOLACE_INBOUND_MESSAGE_SETTLED} in the connector module.
     */
    public static final String SOLACE_INBOUND_MESSAGE_SETTLED = "solace.inbound.message.settled";

    /**
     * Prefix for user-defined properties carried in the Solace SDTMap
     * ({@code message.getProperties()}).
     * Each key is surfaced as {@code solace.userProp.<key>} on the MessageContext.
     * For example, a user property named "region" becomes "solace.userProp.region".
     */
    public static final String SOLACE_USER_PROP_PREFIX = "solace.userProp.";
}
