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

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DurableTopicEndpoint;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.PollingConstants;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericEventBasedConsumer;

import java.util.Properties;

import static org.wso2.carbon.inbound.solace.SolaceUtils.getBooleanProperty;
import static org.wso2.carbon.inbound.solace.SolaceUtils.getIntProperty;
import static org.wso2.carbon.inbound.solace.SolaceUtils.validateRequiredParam;

/**
 * Solace inbound endpoint listener that consumes messages from a Solace Event Broker.
 *
 * <p>Supports three destination types controlled by the {@code solace.destinationType} parameter:
 * <ul>
 *   <li><b>QUEUE</b> — Guaranteed point-to-point delivery from a provisioned queue.</li>
 *   <li><b>TOPIC (Direct)</b> — Non-persistent fanout to all active topic subscribers.
 *       Accepts comma-separated topic strings for multiple subscriptions.</li>
 *   <li><b>TOPIC (Durable)</b> — Persistent topic delivery via a Durable Topic Endpoint (DTE).</li>
 * </ul>
 */
public class SolaceEventListener extends GenericEventBasedConsumer implements XMLMessageListener {

    private static final Log log = LogFactory.getLog(SolaceEventListener.class);

    // JCSMP resources
    private JCSMPSession session;
    private FlowReceiver flowReceiver;
    private XMLMessageConsumer xmlMessageConsumer;

    // Message injection handler
    private SolaceInjectHandler injectHandler;

    // Lifecycle state — all access is synchronized on this instance
    private volatile boolean isConnected = false;

    // Connection Configurations
    private final String host;
    private final String vpnName;
    private final String username;
    private final String apiProvidedUsername;
    private final String password;
    private final String clientName;
    private final int connectRetries;
    private final int connectRetriesPerHost;
    private final int reconnectRetries;
    private final int reconnectRetryWaitMillis;
    private final int connectionTimeoutMillis;

    // Authentication scheme
    private final String authScheme;
    private final String oauth2AccessToken;
    private final String oauth2IssuerIdentifier;
    private final String oidcIdToken;

    // SSL/TLS Configurations
    private final String sslTrustStorePath;
    private final String sslTrustStorePassword;
    private final String sslTrustStoreFormat;
    private final String sslKeyStorePath;
    private final String sslKeyStorePassword;
    private final String sslKeyStoreFormat;
    private final String sslKeyPassword;
    private final boolean sslValidateCertificate;
    private final boolean sslValidateCertificateDate;
    private final boolean generateReceiveTimestamps;

    // Destination Configurations
    private final String destinationType;
    private final String destinationName;
    private final String subscriptionType;
    private final String dteName;

    // Consumer Behavior Configurations
    private final boolean autoAck;
    private final int subAckWindowSize;
    private final String failureOutcome;
    private final boolean binaryPayloadAsBase64;
    private final String selector;

    // Queue provisioning
    private final boolean provisionDestination;
    private final String queueAccessType;
    private final String queuePermission;
    private final int queueQuotaMB;
    private final int queueMaxMsgSize;
    private final boolean queueRespectTTL;
    private final int queueMaxMsgRedelivery;

    // Ingestion Configurations
    private final String contentType;

    public SolaceEventListener(Properties properties, String name,
                               SynapseEnvironment synapseEnvironment,
                               String injectingSeq, String onErrorSeq,
                               boolean coordination, boolean sequential) {
        super(properties, name, synapseEnvironment, injectingSeq, onErrorSeq,
                coordination, sequential);

        // Connection
        this.host       = properties.getProperty(SolaceInboundConstants.HOST);
        this.vpnName    = properties.getProperty(SolaceInboundConstants.VPN_NAME, "default");
        this.username   = properties.getProperty(SolaceInboundConstants.USERNAME);
        this.apiProvidedUsername = properties.getProperty(SolaceInboundConstants.API_PROVIDED_USERNAME);
        this.password   = properties.getProperty(SolaceInboundConstants.PASSWORD);
        this.clientName = properties.getProperty(SolaceInboundConstants.CLIENT_NAME);

        // Authentication scheme
        this.authScheme             = properties.getProperty(
                SolaceInboundConstants.AUTH_SCHEME,
                SolaceInboundConstants.AUTH_SCHEME_BASIC);
        this.oauth2AccessToken      = properties.getProperty(SolaceInboundConstants.OAUTH2_ACCESS_TOKEN);
        this.oauth2IssuerIdentifier = properties.getProperty(SolaceInboundConstants.OAUTH2_ISSUER_IDENTIFIER);
        this.oidcIdToken            = properties.getProperty(SolaceInboundConstants.OIDC_ID_TOKEN);

        // Connection retry
        this.connectionTimeoutMillis  = getIntProperty(properties,
                SolaceInboundConstants.CONNECTION_TIMEOUT, 30000);
        this.connectRetries           = getIntProperty(properties,
                SolaceInboundConstants.CONNECT_RETRIES, 3);
        this.connectRetriesPerHost    = getIntProperty(properties,
                SolaceInboundConstants.CONNECT_RETRIES_PER_HOST, 3);
        this.reconnectRetries         = getIntProperty(properties,
                SolaceInboundConstants.RECONNECT_RETRIES, 3);
        this.reconnectRetryWaitMillis = getIntProperty(properties,
                SolaceInboundConstants.RECONNECT_RETRY_WAIT, 3000);

        // SSL/TLS
        this.sslTrustStorePath          = properties.getProperty(SolaceInboundConstants.SSL_TRUST_STORE_PATH);
        this.sslTrustStorePassword      = properties.getProperty(SolaceInboundConstants.SSL_TRUST_STORE_PASSWORD);
        this.sslTrustStoreFormat        = properties.getProperty(SolaceInboundConstants.SSL_TRUST_STORE_FORMAT);
        this.sslKeyStorePath            = properties.getProperty(SolaceInboundConstants.SSL_KEY_STORE_PATH);
        this.sslKeyStorePassword        = properties.getProperty(SolaceInboundConstants.SSL_KEY_STORE_PASSWORD);
        this.sslKeyStoreFormat          = properties.getProperty(SolaceInboundConstants.SSL_KEY_STORE_FORMAT);
        this.sslKeyPassword             = properties.getProperty(SolaceInboundConstants.SSL_KEY_PASSWORD);
        this.sslValidateCertificate     = getBooleanProperty(properties,
                SolaceInboundConstants.SSL_VALIDATE_CERTIFICATE, true);
        this.sslValidateCertificateDate = getBooleanProperty(properties,
                SolaceInboundConstants.SSL_VALIDATE_CERTIFICATE_DATE, true);
        this.generateReceiveTimestamps  = getBooleanProperty(properties,
                SolaceInboundConstants.GENERATE_RECEIVE_TIMESTAMPS, false);

        // Destination
        this.destinationType  = properties.getProperty(SolaceInboundConstants.DESTINATION_TYPE);
        this.destinationName  = properties.getProperty(SolaceInboundConstants.DESTINATION_NAME);
        this.subscriptionType = properties.getProperty(
                SolaceInboundConstants.SUBSCRIPTION_TYPE,
                SolaceInboundConstants.SUBSCRIPTION_TYPE_DIRECT);
        this.dteName = properties.getProperty(SolaceInboundConstants.DTE_NAME);

        // Consumer behaviour
        this.autoAck              = getBooleanProperty(properties, SolaceInboundConstants.AUTO_ACK, true);
        this.subAckWindowSize     = getIntProperty(properties,
                SolaceInboundConstants.SUB_ACK_WINDOW_SIZE, 255);
        this.failureOutcome       = properties.getProperty(
                SolaceInboundConstants.FAILURE_OUTCOME,
                SolaceInboundConstants.FAILURE_OUTCOME_NONE);
        this.binaryPayloadAsBase64 = getBooleanProperty(properties,
                SolaceInboundConstants.BINARY_PAYLOAD_AS_BASE64, false);
        this.selector = properties.getProperty(SolaceInboundConstants.SELECTOR);

        // Queue provisioning
        this.provisionDestination = getBooleanProperty(properties,
                SolaceInboundConstants.PROVISION_DESTINATION, false);
        this.queueAccessType      = properties.getProperty(
                SolaceInboundConstants.QUEUE_ACCESS_TYPE,
                SolaceInboundConstants.QUEUE_ACCESS_EXCLUSIVE);
        this.queuePermission      = properties.getProperty(
                SolaceInboundConstants.QUEUE_PERMISSION);
        this.queueQuotaMB         = getIntProperty(properties,
                SolaceInboundConstants.QUEUE_QUOTA_MB, 0);
        this.queueMaxMsgSize      = getIntProperty(properties,
                SolaceInboundConstants.QUEUE_MAX_MSG_SIZE, 0);
        this.queueRespectTTL      = getBooleanProperty(properties,
                SolaceInboundConstants.QUEUE_RESPECT_TTL, false);
        this.queueMaxMsgRedelivery = getIntProperty(properties,
                SolaceInboundConstants.QUEUE_MAX_MSG_REDELIVERY, 0);

        // Ingestion
        this.contentType = properties.getProperty(SolaceInboundConstants.CONTENT_TYPE);

        // Execution behaviour
        this.sequential = BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(
                        properties.getProperty(PollingConstants.INBOUND_ENDPOINT_SEQUENTIAL)),
                sequential);
        this.coordination = BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(
                        properties.getProperty(PollingConstants.INBOUND_COORDINATION)),
                coordination);
    }

    @Override
    public synchronized void listen() {
        if (isConnected && session != null && !session.isClosed()) {
            if (log.isDebugEnabled()) {
                log.debug("SolaceListener [" + name + "] already connected, "
                        + "skipping re-initialisation.");
            }
            return;
        }

        try {
            session = JCSMPFactory.onlyInstance().createSession(
                    buildJCSMPProperties(), null,
                    new SolaceSessionEventHandler(name));
            session.connect();
            isConnected = true;

            injectHandler = new SolaceInjectHandler(
                    injectingSeq, onErrorSeq, synapseEnvironment, sequential,
                    contentType, binaryPayloadAsBase64);

            initializeConsumer();

            log.info("SolaceListener [" + name + "] started successfully. "
                    + "Type=" + destinationType + ", Destination=" + destinationName);

        } catch (JCSMPException e) {
            cleanupSessionQuietly();
            log.error("Failed to initialise SolaceListener [" + name + "]", e);
            throw new SynapseException(
                    "Failed to initialise SolaceListener [" + name + "]", e);
        }
    }

    public synchronized void pause() {
        log.info("Pausing SolaceListener [" + name + "]...");
        destroy();
    }

    public synchronized void resume() {
        if (!isConnected) {
            log.info("Resuming SolaceListener [" + name + "]...");
            listen();
        }
    }

    @Override
    public synchronized void destroy() {
        if (!isConnected) {
            return;
        }
        log.info("Destroying SolaceListener [" + name + "]...");
        try {
            if (flowReceiver != null) {
                flowReceiver.close();
                flowReceiver = null;
            }
            if (xmlMessageConsumer != null) {
                xmlMessageConsumer.close();
                xmlMessageConsumer = null;
            }
            if (session != null && !session.isClosed()) {
                session.closeSession();
                session = null;
            }
        } catch (Exception e) {
            log.warn("Error during SolaceListener [" + name + "] shutdown", e);
        }
        isConnected = false;
        log.info("SolaceListener [" + name + "] destroyed.");
    }

    /**
     * Closes the session without touching isConnected — used when listen() fails
     * after session.connect() but before the listener is fully wired.
     */
    private void cleanupSessionQuietly() {
        try {
            if (session != null && !session.isClosed()) {
                session.closeSession();
            }
        } catch (Exception e) {
            log.debug("Error closing session during cleanup", e);
        }
        session = null;
        isConnected = false;
        flowReceiver = null;
        xmlMessageConsumer = null;
    }

    private void initializeQueueConsumer() throws JCSMPException {
        validateRequiredParam(destinationName, SolaceInboundConstants.DESTINATION_NAME,
                SolaceInboundConstants.DESTINATION_TYPE_QUEUE);
        Queue queue = JCSMPFactory.onlyInstance().createQueue(destinationName);

        if (provisionDestination) {
            session.provision(queue, buildEndpointProperties(),
                    JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
        }

        ConsumerFlowProperties flowProps = buildFlowProperties(queue);
        flowReceiver = session.createFlow(this, flowProps, null,
                new SolaceFlowEventHandler(name, this::destroy));
        flowReceiver.start();
        log.info("SolaceListener [" + name + "] consuming from queue: " + destinationName);
    }

    /**
     * Subscribes to one or more topics (comma-separated in destinationName).
     */
    private void initializeTopicConsumer() throws JCSMPException {
        validateRequiredParam(destinationName, SolaceInboundConstants.DESTINATION_NAME,
                SolaceInboundConstants.DESTINATION_TYPE_TOPIC);
        xmlMessageConsumer = session.getMessageConsumer(this);

        String[] topicStrings = destinationName.split(",");
        for (String topicStr : topicStrings) {
            String trimmed = topicStr.trim();
            if (!trimmed.isEmpty()) {
                Topic topic = JCSMPFactory.onlyInstance().createTopic(trimmed);
                session.addSubscription(topic);
                log.info("SolaceListener [" + name + "] subscribed to topic: " + trimmed);
            }
        }

        xmlMessageConsumer.start();
    }

    private void initializeDurableTopicConsumer() throws JCSMPException {
        String context = SolaceInboundConstants.DESTINATION_TYPE_TOPIC
                + " + " + SolaceInboundConstants.SUBSCRIPTION_TYPE_DURABLE;
        validateRequiredParam(destinationName, SolaceInboundConstants.DESTINATION_NAME, context);
        validateRequiredParam(dteName, SolaceInboundConstants.DTE_NAME, context);

        DurableTopicEndpoint dte = (DurableTopicEndpoint) JCSMPFactory.onlyInstance()
                .createDurableTopicEndpointEx(dteName);

        if (provisionDestination) {
            session.provision(dte, buildEndpointProperties(),
                    JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
        }

        ConsumerFlowProperties flowProps = buildFlowProperties(dte);
        flowProps.setNewSubscription(
                JCSMPFactory.onlyInstance().createTopic(destinationName));
        flowReceiver = session.createFlow(this, flowProps, null,
                new SolaceFlowEventHandler(name, this::destroy));
        flowReceiver.start();
        log.info("SolaceListener [" + name + "] consuming from DTE: " + dteName
                + ", subscription: " + destinationName);
    }

    private void initializeConsumer() throws JCSMPException {
        if (SolaceInboundConstants.DESTINATION_TYPE_QUEUE
                .equalsIgnoreCase(destinationType)) {
            initializeQueueConsumer();
        } else if (SolaceInboundConstants.DESTINATION_TYPE_TOPIC
                .equalsIgnoreCase(destinationType)
                && SolaceInboundConstants.SUBSCRIPTION_TYPE_DURABLE
                .equalsIgnoreCase(subscriptionType)) {
            initializeDurableTopicConsumer();
        } else if (SolaceInboundConstants.DESTINATION_TYPE_TOPIC
                .equalsIgnoreCase(destinationType)) {
            initializeTopicConsumer();
        } else {
            throw new SynapseException(
                    "Unsupported destinationType: '" + destinationType
                    + "'. Valid values: QUEUE, TOPIC.");
        }
    }

    private JCSMPProperties buildJCSMPProperties() {
        JCSMPProperties props = new JCSMPProperties();

        // Core connection
        props.setProperty(JCSMPProperties.HOST, host);
        props.setProperty(JCSMPProperties.VPN_NAME, vpnName);
        props.setProperty(JCSMPProperties.USERNAME, username);
        props.setProperty(JCSMPProperties.PASSWORD, password);

        if (clientName != null) {
            props.setProperty(JCSMPProperties.CLIENT_NAME, clientName);
        }

        // Authentication scheme
        applyAuthScheme(props);

        // Connection retry
        JCSMPChannelProperties channelProps = (JCSMPChannelProperties)
                props.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
        channelProps.setConnectTimeoutInMillis(connectionTimeoutMillis);
        channelProps.setConnectRetries(connectRetries);
        channelProps.setConnectRetriesPerHost(connectRetriesPerHost);
        channelProps.setReconnectRetries(reconnectRetries);
        channelProps.setReconnectRetryWaitInMillis(reconnectRetryWaitMillis);

        // Transport window size
        props.setProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE, subAckWindowSize);

        props.setProperty(JCSMPProperties.GENERATE_RCV_TIMESTAMPS, generateReceiveTimestamps);

        // SSL/TLS
        if (sslTrustStorePath != null) {
            props.setProperty(JCSMPProperties.SSL_TRUST_STORE, sslTrustStorePath);
            if (sslTrustStorePassword != null) {
                props.setProperty(JCSMPProperties.SSL_TRUST_STORE_PASSWORD,
                        sslTrustStorePassword);
            }
            if (sslTrustStoreFormat != null) {
                props.setProperty(JCSMPProperties.SSL_TRUST_STORE_FORMAT,
                        sslTrustStoreFormat);
            }
            props.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE,
                    sslValidateCertificate);
            props.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE,
                    sslValidateCertificateDate);
        }

        // Mutual TLS
        if (sslKeyStorePath != null) {
            props.setProperty(JCSMPProperties.SSL_KEY_STORE, sslKeyStorePath);
            if (sslKeyStorePassword != null) {
                props.setProperty(JCSMPProperties.SSL_KEY_STORE_PASSWORD, sslKeyStorePassword);
            }
            if (sslKeyStoreFormat != null) {
                props.setProperty(JCSMPProperties.SSL_KEY_STORE_FORMAT, sslKeyStoreFormat);
            }
            if (sslKeyPassword != null) {
                props.setProperty(JCSMPProperties.SSL_PRIVATE_KEY_PASSWORD, sslKeyPassword);
            }
        }

        return props;
    }

    private void applyAuthScheme(JCSMPProperties props) {
        if (SolaceInboundConstants.AUTH_SCHEME_CLIENT_CERTIFICATE
                .equalsIgnoreCase(authScheme)) {
            props.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME,
                    JCSMPProperties.AUTHENTICATION_SCHEME_CLIENT_CERTIFICATE);
            // JCSMP requires a non-null USERNAME to build the SMF login frame, even when authentication is by client certificate. 
            if (apiProvidedUsername != null && !apiProvidedUsername.trim().isEmpty()) {     
                props.setProperty(JCSMPProperties.USERNAME, apiProvidedUsername);
            } else {
               // JCSMP NPEs on null USERNAME during SMF login frame build; broker ignores this
               // value when username-source=common-name (default) and derives the identity from the TLS-presented cert.
               props.setProperty(JCSMPProperties.USERNAME, "<client-cert-cn>");
            }
        } else if (SolaceInboundConstants.AUTH_SCHEME_OAUTH2
                .equalsIgnoreCase(authScheme)) {
            props.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME,
                    JCSMPProperties.AUTHENTICATION_SCHEME_OAUTH2);
            if (oauth2AccessToken != null) {
                props.setProperty(JCSMPProperties.OAUTH2_ACCESS_TOKEN, oauth2AccessToken);
            }
            if (oauth2IssuerIdentifier != null) {
                props.setProperty(JCSMPProperties.OAUTH2_ISSUER_IDENTIFIER, oauth2IssuerIdentifier);
            }
            if (oidcIdToken != null) {
                props.setProperty(JCSMPProperties.OIDC_ID_TOKEN, oidcIdToken);
            }
            // JCSMP NPEs on null USERNAME during SMF login frame build; broker ignores this value when using OAuth2 authentication.
            // A dummy value to avoid the NPE; the real OAuth2 token is sent in the OAUTH2_ACCESS_TOKEN property and the broker authenticates based on that.
            props.setProperty(JCSMPProperties.USERNAME, "<oauth-token>");
        } else {
            props.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME,
                    JCSMPProperties.AUTHENTICATION_SCHEME_BASIC);
        }
    }

    private ConsumerFlowProperties buildFlowProperties(Endpoint endpoint) {
        ConsumerFlowProperties flowProps = new ConsumerFlowProperties();
        flowProps.setEndpoint(endpoint);
        flowProps.setAckMode(autoAck
                ? JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO
                : JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
        if (selector != null && !selector.trim().isEmpty()) {
            flowProps.setSelector(selector.trim());
        }        
        return flowProps;
    }

    /**
     * Builds EndpointProperties for queue/DTE provisioning from user config.
     */
    private EndpointProperties buildEndpointProperties() {
        EndpointProperties endpointProps = new EndpointProperties();
        if (SolaceInboundConstants.QUEUE_ACCESS_NON_EXCLUSIVE.equalsIgnoreCase(queueAccessType)) {
            endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
        } else {
            endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
        }
        if ("CONSUME".equalsIgnoreCase(queuePermission)) {
            endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
        } else if ("DELETE".equalsIgnoreCase(queuePermission)) {
            endpointProps.setPermission(EndpointProperties.PERMISSION_DELETE);
        } else if ("MODIFY_TOPIC".equalsIgnoreCase(queuePermission)) {
            endpointProps.setPermission(EndpointProperties.PERMISSION_MODIFY_TOPIC);
        } else if ("READ_ONLY".equalsIgnoreCase(queuePermission)) {
            endpointProps.setPermission(EndpointProperties.PERMISSION_READ_ONLY);
        }
        if (queueQuotaMB > 0) {
            endpointProps.setQuota(queueQuotaMB);
        }
        if (queueMaxMsgSize > 0) {
            endpointProps.setMaxMsgSize(queueMaxMsgSize);
        }
        endpointProps.setRespectsMsgTTL(queueRespectTTL);
        if (queueMaxMsgRedelivery > 0) {
            endpointProps.setMaxMsgRedelivery(queueMaxMsgRedelivery);
        }
        return endpointProps;
    }

    @Override
    public void onReceive(BytesXMLMessage message) {
        if (injectHandler == null) {
            log.warn("injectHandler null, cannot process message.");
            return;
        }

        log.info("Received message with HTTP content type: " + message.getHTTPContentType() + " and content: " + message.hasContent() + " and binary payload as base64: " + binaryPayloadAsBase64);

        boolean success = injectHandler.injectMessage(message);

        if (!autoAck) {
            if (success) {
                message.ackMessage();
            } else {
                handleFailedMessage(message);
            }
        }
    }

    private void handleFailedMessage(BytesXMLMessage message) {
        try {
            if (SolaceInboundConstants.FAILURE_OUTCOME_FAILED
                    .equalsIgnoreCase(failureOutcome)) {
                message.settle(XMLMessage.Outcome.FAILED);
                log.warn("Settled message [" + message.getMessageId()
                        + "] as FAILED (will be redelivered).");
            } else if (SolaceInboundConstants.FAILURE_OUTCOME_REJECTED
                    .equalsIgnoreCase(failureOutcome)) {
                message.settle(XMLMessage.Outcome.REJECTED);
                log.warn("Settled message [" + message.getMessageId()
                        + "] as REJECTED (routed to DMQ).");
            } else {
                log.warn("Injection failed for message [" + message.getMessageId()
                        + "], redelivered=" + message.getRedelivered()
                        + ". No ACK — broker redelivery limit applies.");
            }
        } catch (JCSMPException e) {
            log.error("Failed to settle message [" + message.getMessageId() + "]", e);
        }
    }

    /**
     * Called by JCSMP when all internal reconnect retries are exhausted.
     * Tears down the listener so the MI framework can trigger resume.
     */
    @Override
    public void onException(JCSMPException exception) {
        log.error("SolaceListener [" + name + "] encountered an error", exception);
        destroy();
    }
}
