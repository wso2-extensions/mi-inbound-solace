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

import com.solace.messaging.trace.propagation.SolaceJCSMPTextMapGetter;
import com.solacesystems.common.util.ByteArray;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.Mediator;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.aspects.flow.statistics.collectors.RuntimeStatisticCollector;
import org.apache.synapse.aspects.flow.statistics.tracing.opentelemetry.management.handling.span.SpanHandler;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.mediators.MediatorFaultHandler;
import org.apache.synapse.mediators.base.SequenceMediator;

import java.io.IOException;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles injection of Solace messages into the Synapse engine.
 */
public class SolaceInjectHandler {

    private static final Log log = LogFactory.getLog(SolaceInjectHandler.class);

    /**
     * Outcome of an injection attempt.
     */
    public enum InjectionOutcome {
        SUCCESS,
        FAILED
    }

    private final String injectingSequence;
    private final String onErrorSequence;
    private final SynapseEnvironment synapseEnvironment;
    private final boolean sequential;
    private final String contentType;
    private final boolean binaryPayloadAsBase64;
    private final String inboundEndpointName;

    /**
     * Reads W3C trace context out of a JCSMP message.
     */
    private static final SolaceJCSMPTextMapGetter TRACE_CONTEXT_GETTER = new SolaceJCSMPTextMapGetter();

    // Delivery count is a per-message capability that may be unsupported on the endpoint. Log the
    // gap once (not per message) — the broker capability is constant for this handler's lifetime.
    private final AtomicBoolean deliveryCountUnsupportedLogged = new AtomicBoolean(false);

    public SolaceInjectHandler(String injectingSequence, String onErrorSequence,
                               SynapseEnvironment synapseEnvironment, boolean sequential,
                               String contentType, boolean binaryPayloadAsBase64,
                               String inboundEndpointName) {
        this.injectingSequence = injectingSequence;
        this.onErrorSequence = onErrorSequence;
        this.synapseEnvironment = synapseEnvironment;
        this.sequential = sequential;
        this.contentType = contentType;
        this.binaryPayloadAsBase64 = binaryPayloadAsBase64;
        this.inboundEndpointName = inboundEndpointName;
    }

    /**
     * Worker thread. Reads the JCSMP message once per delivery, the body and the user properties, and returns
     * the snapshot every attempt builds its context from. Throws when a text body does not decode with its
     * charset, or when a legacy JSON builder would get a body that is not one complete JSON value.
     */
    public SolaceUtils.Payload readPayload(BytesXMLMessage message) throws IOException {
        return SolaceUtils.readPayload(synapseEnvironment, message, contentType, binaryPayloadAsBase64,
                readUserProperties(message));
    }

    /**
     * Worker thread. Builds a fresh context from the snapshot of the delivery, never from the message body
     * again. Returns null for an empty payload (logged WARN), throws for any other build failure.
     */
    public MessageContext buildContext(SolaceUtils.Payload payload, BytesXMLMessage message, int attempt)
            throws Exception {
        if (payload.isEmpty()) {
            log.warn("Received empty Solace message, no mediation context built");
            return null;
        }

        MessageContext synCtx = SolaceUtils.createMessageContext(synapseEnvironment, payload);

        // Names the mediation flow's entry span after the inbound endpoint.
        synCtx.setProperty(SynapseConstants.INBOUND_ENDPOINT_NAME, inboundEndpointName);
        synCtx.setProperty(SynapseConstants.IS_INBOUND, true);

        if (RuntimeStatisticCollector.isOpenTelemetryEnabled()) {
            try {
                SpanHandler.extractTraceContextAndInjectToMessageContext(message, synCtx, TRACE_CONTEXT_GETTER);
            } catch (NoSuchMethodError e) {
                log.warn("You are running solace inbound connector on old wso2mi version. " +
                        "Distributed tracing will not working correctly in this version. " +
                        "Please upgrade the wso2mi version");
            }
        }
        // Stash the raw JCSMP message so sendReply / acknowledgeMessage / nackMessage
        // can access the original handle from the mediation flow.
        synCtx.setProperty(SolaceInboundConstants.SOLACE_INBOUND_MESSAGE, message);

        setMessageProperties(synCtx, message);
        setUserProperties(synCtx, payload);
        synCtx.setProperty(SolaceInboundConstants.SOLACE_DELIVERY_ATTEMPT, String.valueOf(attempt));
        synCtx.setProperty(SolaceInboundConstants.SOLACE_REDELIVERED,
                String.valueOf(message.getRedelivered() || attempt > 1));

        // Wire up the error sequence via pushFaultHandler so it is actually invoked on errors
        if (StringUtils.isNotEmpty(onErrorSequence)) {
            Mediator errorSeq = synapseEnvironment
                    .getSynapseConfiguration().getSequence(onErrorSequence);
            if (errorSeq != null) {
                synCtx.pushFaultHandler(new MediatorFaultHandler(errorSeq));
            }
        }
        return synCtx;
    }

    /**
     * Worker thread. Never throws. sequential is always true.
     */
    public InjectionOutcome inject(MessageContext synCtx) {
        try {
            if (StringUtils.isNotEmpty(injectingSequence)) {
                SequenceMediator seq = (SequenceMediator) synapseEnvironment
                        .getSynapseConfiguration().getSequence(injectingSequence);
                if (seq == null) {
                    log.error("Sequence '" + injectingSequence + "' not found.");
                    return InjectionOutcome.FAILED;
                }
                if (log.isDebugEnabled()) {
                    log.debug("Injecting Solace message into sequence: " + injectingSequence);
                }
                synapseEnvironment.injectInbound(synCtx, seq, true);
            } else {
                synapseEnvironment.injectMessage(synCtx);
            }
            return InjectionOutcome.SUCCESS;
        } catch (Exception e) {
            log.error("Error injecting Solace message into Synapse engine", e);
            return InjectionOutcome.FAILED;
        }
    }

    private void setMessageProperties(org.apache.synapse.MessageContext synCtx,
                                      BytesXMLMessage message) {

        // Envelope (broker-set)
        if (message.getMessageId() != null) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_MESSAGE_ID, message.getMessageId());
        }

        Destination destination = message.getDestination();
        if (destination != null) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_DESTINATION, destination.getName());
        }

        if (message.getDeliveryMode() != null) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_DELIVERY_MODE,
                    message.getDeliveryMode().name());

            if (message.getDeliveryMode() != DeliveryMode.DIRECT) {
                synCtx.setProperty(SolaceInboundConstants.SOLACE_REDELIVERED,
                        String.valueOf(message.getRedelivered()));

                try {
                    synCtx.setProperty(SolaceInboundConstants.SOLACE_DELIVERY_COUNT,
                            String.valueOf(message.getDeliveryCount()));
                } catch (UnsupportedOperationException e) {
                    // Capability gap, not an error — the message is processed normally; only the
                    // optional delivery-count property is omitted. Log once to avoid per-message noise.
                    if (deliveryCountUnsupportedLogged.compareAndSet(false, true)) {
                        log.warn("Delivery count is not supported on this broker endpoint; the '"
                                + SolaceInboundConstants.SOLACE_DELIVERY_COUNT
                                + "' property will be omitted.");
                    }
                }
            }
        }

        if (message.getReceiveTimestamp() != 0) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_RECEIVE_TIMESTAMP,
                    String.valueOf(message.getReceiveTimestamp()));
        }

        if (message.getSequenceNumber() != null) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_SEQUENCE_NUMBER,
                    String.valueOf(message.getSequenceNumber()));
        }

        if (message.getExpiration() != 0) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_EXPIRATION,
                    String.valueOf(message.getExpiration()));
        }

        synCtx.setProperty(SolaceInboundConstants.SOLACE_DISCARD_INDICATION,
                String.valueOf(message.getDiscardIndication()));

        // Publisher-set header
        synCtx.setProperty(SolaceInboundConstants.SOLACE_PRIORITY, message.getPriority());

        if (message.getCorrelationId() != null) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_CORRELATION_ID,
                    message.getCorrelationId());
        }

        if (message.getReplyTo() != null) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_REPLY_TO,
                    message.getReplyTo().getName());
        }

        if (message.getSenderId() != null) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_SENDER_ID, message.getSenderId());
        }

        if (message.getSenderTimestamp() != null) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_SENDER_TIMESTAMP,
                    message.getSenderTimestamp().toString());
        }

        if (message.getApplicationMessageId() != null) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_APPLICATION_MESSAGE_ID,
                    message.getApplicationMessageId());
        }

        if (message.getApplicationMessageType() != null) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_APPLICATION_MESSAGE_TYPE,
                    message.getApplicationMessageType());
        }

        if (message.getTimeToLive() != 0) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_TIME_TO_LIVE,
                    String.valueOf(message.getTimeToLive()));
        }

        // HTTP bridging headers
        if (StringUtils.isNotEmpty(message.getHTTPContentType())) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_HTTP_CONTENT_TYPE,
                    message.getHTTPContentType());
        }

        if (StringUtils.isNotEmpty(message.getHTTPContentEncoding())) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_HTTP_CONTENT_ENCODING,
                    message.getHTTPContentEncoding());
        }

        // QoS / routing flags
        synCtx.setProperty(SolaceInboundConstants.SOLACE_COS,
                String.valueOf(message.getCos()));

        synCtx.setProperty(SolaceInboundConstants.SOLACE_DMQ_ELIGIBLE,
                String.valueOf(message.isDMQEligible()));

        synCtx.setProperty(SolaceInboundConstants.SOLACE_ELIDING_ELIGIBLE,
                String.valueOf(message.isElidingEligible()));
    }

    /**
     * User defined properties (SDTMap), set from the snapshot of the delivery.
     */
    private static void setUserProperties(MessageContext synCtx, SolaceUtils.Payload payload) {
        for (Map.Entry<String, String> property : payload.userProperties.entrySet()) {
            synCtx.setProperty(SolaceInboundConstants.SOLACE_USER_PROP_PREFIX + property.getKey(),
                    property.getValue());
        }
    }

    /**
     * Reads the user properties once per delivery. A nested map or stream is read here and never again.
     */
    private static Map<String, String> readUserProperties(BytesXMLMessage message) {
        Map<String, String> properties = new LinkedHashMap<>();
        SDTMap userProps;
        try {
            userProps = message.getProperties();
        } catch (Exception e) {
            log.warn("Could not retrieve Solace user properties.", e);
            return properties;
        }

        if (userProps == null) {
            return properties;
        }

        Iterator<String> keys = userProps.keySet().iterator();
        while (keys.hasNext()) {
            String key = keys.next();
            try {
                Object value = userProps.get(key);
                if (value != null) {
                    properties.put(key, userPropertyText(value));
                }
            } catch (SDTException e) {
                log.warn("Could not read Solace user property '" + key + "', skipping.", e);
            }
        }
        return properties;
    }

    /**
     * A byte value, a JCSMP ByteArray, becomes base64 text. Every other type keeps its 1.x string form.
     */
    private static String userPropertyText(Object value) {
        if (value instanceof ByteArray) {
            return Base64.getEncoder().encodeToString(((ByteArray) value).asBytes());
        }
        return String.valueOf(value);
    }
}
