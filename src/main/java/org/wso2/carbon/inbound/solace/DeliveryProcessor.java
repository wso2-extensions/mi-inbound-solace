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
import com.solacesystems.jcsmp.XMLMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.wso2.carbon.inbound.solace.hooks.Hooks;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_AUTOACK;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_DRAIN;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_INJECTION_REJECTED;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_OTHER;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_TIMEOUT;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.SOLACE_INBOUND_SETTLEMENT;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.VALUE_ACK;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.VALUE_FAILED;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.VALUE_REJECTED;

final class DeliveryProcessor {

    private static final Log log = LogFactory.getLog(DeliveryProcessor.class);

    enum Decision { ACK, FAILED, REJECTED, NONE }

    private DeliveryProcessor() {
    }

    static void process(SolaceEventListener listener, Generation generation, Generation.Delivery delivery) {
        Generation.Delivery current = delivery;
        boolean released = false;
        try {
            while (current != null) {
                current.worker = Thread.currentThread();
                handleOne(listener, generation, current);
                current = finish(listener, generation, current);
            }
            released = true;
        } finally {
            if (!released) {
                releaseAfterError(listener, generation, current);
            }
        }
    }

    static void releaseAfterError(SolaceEventListener listener, Generation generation, Generation.Delivery dying) {
        Counters counters = listener.counters;
        synchronized (generation.admission) {
            if (dying != null) {
                generation.inFlightDeliveries.remove(dying);
            }
            Generation.Parked parked = (!generation.retired && !generation.draining)
                    ? generation.pending.pollFirst() : null;
            if (parked != null) {
                Generation.Delivery next = new Generation.Delivery(parked.msg, parked.receivedNanos);
                generation.inFlightDeliveries.add(next);
                try {
                    generation.workers.execute(() -> process(listener, generation, next));
                    counters.gauge(generation).pending.set(generation.pending.size());
                    return;
                } catch (RejectedExecutionException e) {
                    generation.inFlightDeliveries.remove(next);
                    counters.rejectedSubmit.increment();
                    log.warn("SolaceListener [" + listener.name + "] message " + parked.msg.getMessageId()
                            + " not re-dispatched after a worker error; left to broker redelivery");
                }
            }
            generation.freePermits++;
            generation.inFlight--;
            counters.gauge(generation).inFlight.set(generation.inFlight);
            counters.gauge(generation).freePermits.set(generation.freePermits);
            counters.gauge(generation).pending.set(generation.pending.size());
            if (generation.inFlight == 0) {
                generation.admission.notifyAll();
            }
        }
    }

    static void handleOne(SolaceEventListener listener, Generation generation, Generation.Delivery delivery) {
        SolaceInboundConfig config = listener.config;
        Counters counters = listener.counters;
        String name = listener.name;
        BytesXMLMessage message = delivery.msg;
        try {
            int attempt = 1;
            if (message.getRedelivered()) {
                counters.redelivered.increment();
            }
            Decision decision;
            Decision decisionOverride = null;
            // We read the JCSMP message once, before the first attempt. A stream or map message consumes its
            // read cursor on that conversion, so every attempt builds its context from this snapshot and the
            // message body is never read again. A conversion failure is the first attempt's build failure.
            SolaceUtils.Payload payload = null;
            Exception conversionFailure = null;
            try {
                payload = listener.injectHandler.readPayload(message);
            } catch (Exception e) {
                conversionFailure = e;
            }
            while (true) {
                CompletableFuture<String> handle = new CompletableFuture<>();
                synchronized (generation.admission) {
                    if (generation.retired || generation.draining) {
                        counters.leftByDrain.increment();
                        return;
                    }
                    generation.inFlightHandles.put(handle, delivery);
                }
                String value;
                boolean injectionRejected;
                final long attemptStart = System.nanoTime();
                try {
                    MessageContext messageContext = null;
                    Exception buildFailure = conversionFailure;
                    if (buildFailure == null) {
                        try {
                            messageContext = listener.injectHandler.buildContext(payload, message, attempt);
                        } catch (Exception e) {
                            buildFailure = e;
                        }
                    }
                    injectionRejected = (messageContext == null);
                    if (buildFailure != null) {
                        log.error("SolaceListener [" + name + "] message " + message.getMessageId()
                                + " could not be converted into a message context; rejecting", buildFailure);
                    }
                    if (injectionRejected) {
                        if (config.autoAck) {
                            boolean won = handle.complete(SettlementValues.record(CAUSE_AUTOACK));
                            value = won ? CAUSE_AUTOACK : SettlementValues.cause(handle.getNow(null));
                            decisionOverride = Decision.ACK;
                        } else {
                            value = handle.complete(SettlementValues.record(CAUSE_INJECTION_REJECTED))
                                    ? CAUSE_INJECTION_REJECTED : SettlementValues.cause(handle.getNow(null));
                        }
                    } else {
                        messageContext.setProperty(SOLACE_INBOUND_SETTLEMENT, handle);
                        Hooks.get().point("worker.beforeInject", generation, handle);
                        SolaceInjectHandler.InjectionOutcome injection = listener.injectHandler.inject(messageContext);
                        if (config.autoAck) {
                            boolean won = handle.complete(SettlementValues.record(CAUSE_AUTOACK));
                            value = won ? CAUSE_AUTOACK : SettlementValues.cause(handle.getNow(null));
                            decisionOverride = Decision.ACK;
                        } else {
                            value = await(handle, config.settlementTimeoutMillis);
                            if (value == null) {
                                boolean won = handle.complete(SettlementValues.record(CAUSE_TIMEOUT));
                                value = won ? CAUSE_TIMEOUT : SettlementValues.cause(handle.getNow(null));
                                if (won) {
                                    log.warn("SolaceListener [" + name + "] message " + message.getMessageId()
                                            + " not settled within " + config.settlementTimeoutMillis
                                            + " ms (attempt " + attempt + "); applying failureOutcome="
                                            + config.failureOutcome);
                                }
                            }
                        }
                    }
                } finally {
                    synchronized (generation.admission) {
                        generation.inFlightHandles.remove(handle);
                    }
                }
                counters.decided(value).increment();
                Hooks.get().record("attempt.toDecision", System.nanoTime() - attemptStart);
                decision = decisionOverride != null ? decisionOverride : decide(value, config, name);
                if (decision == Decision.FAILED && retryInPlace(config, attempt)) {
                    long delay = backoff(config.redeliveryInitialDelayMillis, config.redeliveryProgression,
                            config.redeliveryMaxDelayMillis, attempt);
                    log.warn("SolaceListener [" + name + "] message " + message.getMessageId() + " attempt " + attempt
                            + " failed; retry in " + delay + " ms");
                    if (!sleepInterruptibly(generation, delay)) {
                        counters.leftByDrain.increment();
                        return;
                    }
                    attempt++;
                    counters.retried.increment();
                    continue;
                }
                break;
            }
            decision = finalDecision(decision, attempt, config, message.getMessageId(), name);
            counters.applied(decision).increment();
            settle(listener, generation, delivery, decision);
        } catch (Exception e) {
            log.error("SolaceListener [" + name + "] unexpected failure while handling message "
                    + message.getMessageId() + "; left unsettled", e);
            counters.retainedUnsettled.increment();
            counters.handlerFailed.increment();
        } catch (Error e) {
            log.error("SolaceListener [" + name + "] error while handling message " + message.getMessageId()
                    + "; left unsettled", e);
            counters.retainedUnsettled.increment();
            counters.handlerFailed.increment();
            throw e;
        }
    }

    static Generation.Delivery finish(SolaceEventListener listener, Generation generation, Generation.Delivery done) {
        SolaceInboundConfig config = listener.config;
        Counters counters = listener.counters;
        synchronized (generation.admission) {
            generation.inFlightDeliveries.remove(done);
            Generation.Delivery next = null;
            if (!generation.retired && !generation.draining) {
                Generation.Parked parked = generation.pending.pollFirst();
                if (parked != null) {
                    next = new Generation.Delivery(parked.msg, parked.receivedNanos);
                    generation.inFlightDeliveries.add(next);
                }
            }
            if (next == null) {
                generation.freePermits++;
                generation.inFlight--;
                counters.gauge(generation).inFlight.set(generation.inFlight);
                counters.gauge(generation).freePermits.set(generation.freePermits);
                if (generation.inFlight == 0) {
                    generation.admission.notifyAll();
                }
            }
            counters.gauge(generation).pending.set(generation.pending.size());
            if (generation.stopped && !generation.draining && generation.pending.size() <= config.lowWaterMark) {
                generation.stopped = false;
                try {
                    Hooks.get().point("dispatcher.beforeRestart", generation);
                    generation.binding.start();
                    log.info("SolaceListener [" + listener.name + "] flow restarted at the low-water mark, pending "
                            + generation.pending.size());
                } catch (Exception e) {
                    log.error("SolaceListener [" + listener.name + "] flow restart failed on generation "
                            + generation.id, e);
                    listener.terminalFailure(generation, "flow restart failed", e);
                }
            }
            return next;
        }
    }

    static String await(CompletableFuture<String> handle, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return null;
            }
            try {
                return SettlementValues.cause(handle.get(remaining, TimeUnit.MILLISECONDS));
            } catch (TimeoutException e) {
                return null;
            } catch (InterruptedException e) {
                Thread.interrupted();
                /* Only the drain interrupts a worker, and only while it sleeps.
                   The wait goes on for the remaining time. */
            } catch (ExecutionException | CancellationException e) {
                return CAUSE_OTHER;
            }
        }
    }

    static Decision outcomeDecision(String outcome) {
        return Decision.valueOf(outcome);
    }

    static Decision decide(String cause, SolaceInboundConfig config, String name) {
        switch (cause) {
            case VALUE_ACK:
                return Decision.ACK;
            case VALUE_FAILED:
                return Decision.FAILED;
            case VALUE_REJECTED:
                return Decision.REJECTED;
            case CAUSE_INJECTION_REJECTED:
                return Decision.REJECTED;
            case CAUSE_AUTOACK:
                return Decision.ACK;
            case CAUSE_TIMEOUT:
                return outcomeDecision(config.failureOutcome);
            case CAUSE_DRAIN:
                return outcomeDecision(config.drainOutcome);
            default:
                log.warn("SolaceListener [" + name + "] unrecognised settlement value '" + cause
                        + "' treated as FAILED");
                return Decision.FAILED;
        }
    }

    static void settle(SolaceEventListener listener, Generation generation, Generation.Delivery delivery,
                       Decision decision) {
        Counters counters = listener.counters;
        String name = listener.name;
        BytesXMLMessage message = delivery.msg;
        if (generation.retired) {
            log.warn("SolaceListener [" + name + "] message " + message.getMessageId() + ": generation " + generation.id
                    + " retired, left to broker redelivery");
            return;
        }
        if (listener.config.direct) {
            return;
        }
        // A settle sent on a flow that is down or reconnecting never reaches the broker, which redelivers.
        String connectivity = listener.health.connectivityWord();
        if (decision != Decision.NONE && !"connected".equals(connectivity)) {
            log.error("SolaceListener [" + name + "] settling message " + message.getMessageId() + " as " + decision
                    + " refused: connectivity is " + connectivity + "; the delivery stays on the flow until unbind");
            counters.settleFailed.increment();
            counters.retainedUnsettled.increment();
            return;
        }
        try {
            Hooks.get().record("delivery.receiptToSettlementCall", System.nanoTime() - delivery.receivedNanos);
            switch (decision) {
                case ACK:
                    message.ackMessage();
                    counters.settledAck.increment();
                    break;
                case FAILED:
                    if (generation.failedSupported) {
                        message.settle(XMLMessage.Outcome.FAILED);
                        counters.settledFailed.increment();
                    } else {
                        retain(listener, generation, message, true);
                    }
                    break;
                case REJECTED:
                    if (!message.isDMQEligible() && listener.loggedNonDmqReject.compareAndSet(false, true)) {
                        log.warn("SolaceListener [" + name + "] message " + message.getMessageId()
                                + " is rejected but not DMQ-eligible; the broker discards it (logged once per "
                                + "endpoint)");
                    }
                    if (generation.rejectedSupported) {
                        message.settle(XMLMessage.Outcome.REJECTED);
                        counters.settledRejected.increment();
                    } else {
                        retain(listener, generation, message, true);
                    }
                    break;
                case NONE:
                    retain(listener, generation, message, false);
                    break;
            }
            Hooks.get().record("delivery.receiptToBrokerReturn", System.nanoTime() - delivery.receivedNanos);
        } catch (Exception e) {
            log.error("SolaceListener [" + name + "] settling message " + message.getMessageId() + " as " + decision
                    + " failed; the delivery stays on the flow until unbind", e);
            counters.settleFailed.increment();
            counters.retainedUnsettled.increment();
        }
    }

    static void retain(SolaceEventListener listener, Generation generation, BytesXMLMessage message, boolean recover) {
        listener.counters.retainedUnsettled.increment();
        if (recover) {
            log.warn("SolaceListener [" + listener.name + "] message " + message.getMessageId()
                    + " retained: the broker lacks the settlement outcome; recovering generation " + generation.id
                    + " so the broker redelivers it (D21)");
            listener.terminalFailure(generation, "retained delivery (degraded capability)", null);
        }
    }

    static boolean retryInPlace(SolaceInboundConfig config, int attempt) {
        if (config.transacted) {
            return false;
        }
        if (config.maxRedeliveries == 0) {
            return false;
        }
        return config.maxRedeliveries < 0 || attempt <= config.maxRedeliveries;
    }

    static long backoff(long initial, double progression, long max, int attempt) {
        return (long) Math.min(initial * Math.pow(progression, attempt - 1), max);
    }

    static Decision finalDecision(Decision decision, int attempt, SolaceInboundConfig config, String messageId,
                                  String name) {
        if (decision != Decision.FAILED) {
            return decision;
        }
        if (config.transacted) {
            return Decision.FAILED;
        }
        if (config.maxRedeliveries == 0) {
            return Decision.FAILED;
        }
        log.error("SolaceListener [" + name + "] message " + messageId + " exhausted " + config.maxRedeliveries
                + " in-place retries; rejecting (DMQ when eligible)");
        return Decision.REJECTED;
    }

    /**
     * Worker thread. Returns false when the drain interrupted the sleep (the message is left unsettled).
     */
    static boolean sleepInterruptibly(Generation generation, long delayMillis) {
        Generation.SleepToken token = new Generation.SleepToken(Thread.currentThread());
        synchronized (generation.admission) {
            if (generation.draining || generation.retired) {
                return false;
            }
            generation.sleepers.add(token);
        }
        boolean natural;
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            /* The drain's signal. The finally block below handles it under the lock. */
        } finally {
            synchronized (generation.admission) {
                natural = token.state.compareAndSet(Generation.SleepToken.SLEEPING, Generation.SleepToken.DONE);
                if (!natural) {
                    Thread.interrupted();
                }
                generation.sleepers.remove(token);
            }
        }
        return natural;
    }
}
