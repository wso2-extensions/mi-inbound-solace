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
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.Mediator;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.mediators.MediatorFaultHandler;
import org.apache.synapse.mediators.base.SequenceMediator;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles injection of Solace messages into the Synapse engine.
 */
public class SolaceInjectHandler {

    private static final Log log = LogFactory.getLog(SolaceInjectHandler.class);

    /**
     * Outcome of an injection attempt. The listener uses this to decide whether to settle
     * the JCSMP message itself after mediation returns.
     */
    public enum InjectionOutcome {
        /** Mediation completed without an injection-time exception. Listener should ack. */
        SUCCESS,
        /** Injection failed with an exception. Listener should apply its failureOutcome policy. */
        FAILED,
        /**
         * The message can never be processed (e.g. empty/unparseable payload). Listener should
         * settle it as REJECTED so the broker routes it to the DMQ — this preserves the full
         * message for inspection without triggering redelivery (which would loop forever).
         */
        REJECTED,
        /** A connector op (acknowledgeMessage / nackMessage) already settled the message during mediation. */
        ALREADY_SETTLED
    }

    private final String injectingSequence;
    private final String onErrorSequence;
    private final SynapseEnvironment synapseEnvironment;
    private final boolean sequential;
    private final String contentType;
    private final boolean binaryPayloadAsBase64;

    // Delivery count is a per-message capability that may be unsupported on the endpoint. Log the
    // gap once (not per message) — the broker capability is constant for this handler's lifetime.
    private final AtomicBoolean deliveryCountUnsupportedLogged = new AtomicBoolean(false);

    public SolaceInjectHandler(String injectingSequence, String onErrorSequence,
                               SynapseEnvironment synapseEnvironment, boolean sequential,
                               String contentType, boolean binaryPayloadAsBase64) {
        this.injectingSequence = injectingSequence;
        this.onErrorSequence = onErrorSequence;
        this.synapseEnvironment = synapseEnvironment;
        this.sequential = sequential;
        this.contentType = contentType;
        this.binaryPayloadAsBase64 = binaryPayloadAsBase64;
    }

    public InjectionOutcome injectMessage(BytesXMLMessage message) {
        org.apache.synapse.MessageContext synCtx = null;
        try {
            String payload = SolaceUtils.extractPayload(message, binaryPayloadAsBase64);
            if (payload == null || payload.isEmpty()) {
                // Nothing to inject. Reject rather than ack so the message (with its headers /
                // user properties intact) is preserved in the DMQ for inspection. Acking would
                // silently drop it; FAILED would redeliver the same empty payload forever.
                log.warn("Received empty Solace message, rejecting (routed to DMQ).");
                return InjectionOutcome.REJECTED;
            }

            synCtx = SolaceUtils.createMessageContext(
                    synapseEnvironment, payload, message, contentType, binaryPayloadAsBase64);

            // Stash the raw JCSMP message so sendReply / acknowledgeMessage / nackMessage
            // can access the original handle from the mediation flow.
            synCtx.setProperty(SolaceInboundConstants.SOLACE_INBOUND_MESSAGE, message);

            setMessageProperties(synCtx, message);

            // Wire up the error sequence via pushFaultHandler so it is actually invoked on errors
            if (StringUtils.isNotEmpty(onErrorSequence)) {
                Mediator errorSeq = synapseEnvironment
                        .getSynapseConfiguration().getSequence(onErrorSequence);
                if (errorSeq != null) {
                    synCtx.pushFaultHandler(new MediatorFaultHandler(errorSeq));
                }
            }

            if (StringUtils.isNotEmpty(injectingSequence)) {
                SequenceMediator seq = (SequenceMediator) synapseEnvironment
                        .getSynapseConfiguration().getSequence(injectingSequence);
                if (seq != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Injecting Solace message into sequence: " + injectingSequence);
                    }
                    synapseEnvironment.injectInbound(synCtx, seq, sequential);
                } else {
                    log.error("Sequence '" + injectingSequence + "' not found.");
                    return InjectionOutcome.FAILED;
                }
            } else {
                synapseEnvironment.injectMessage(synCtx);
            }

            // A connector op (acknowledgeMessage / nackMessage) that ran during mediation settles
            // the JCSMP message directly and sets SOLACE_INBOUND_MESSAGE_SETTLED=true on the
            // context. Manual-ack mode is always sequential, so mediation has finished here and
            // the flag is authoritative. When it is set, return ALREADY_SETTLED so onReceive skips
            // its own post-mediation settle: settling twice would conflict with the broker's
            // redelivery tracking — an ack after a connector nack drops the intended FAILED/
            // REJECTED (redelivery / DMQ) outcome, and a redundant settle can cause infinite
            // redelivery. (A second ack alone is a Solace no-op; the ack-then-nack case is the hazard.)
            if (Boolean.TRUE.equals(
                    synCtx.getProperty(SolaceInboundConstants.SOLACE_INBOUND_MESSAGE_SETTLED))) {
                return InjectionOutcome.ALREADY_SETTLED;
            }
            return InjectionOutcome.SUCCESS;
        } catch (Exception e) {
            log.error("Error injecting Solace message into Synapse engine", e);
            if (synCtx != null && Boolean.TRUE.equals(
                    synCtx.getProperty(SolaceInboundConstants.SOLACE_INBOUND_MESSAGE_SETTLED))) {
                return InjectionOutcome.ALREADY_SETTLED;
            }
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

        // User-defined properties (SDTMap)
        setUserProperties(synCtx, message);
    }

    private void setUserProperties(org.apache.synapse.MessageContext synCtx,
                                   BytesXMLMessage message) {
        SDTMap userProps;
        try {
            userProps = message.getProperties();
        } catch (Exception e) {
            log.warn("Could not retrieve Solace user properties.", e);
            return;
        }

        if (userProps == null) {
            return;
        }

        Iterator<String> keys = userProps.keySet().iterator();
        while (keys.hasNext()) {
            String key = keys.next();
            try {
                Object value = userProps.get(key);
                if (value != null) {
                    synCtx.setProperty(
                            SolaceInboundConstants.SOLACE_USER_PROP_PREFIX + key,
                            String.valueOf(value));
                }
            } catch (SDTException e) {
                log.warn("Could not read Solace user property '" + key + "', skipping.", e);
            }
        }
    }
}
