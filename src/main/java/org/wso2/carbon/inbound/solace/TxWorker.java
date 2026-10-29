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
import com.solacesystems.jcsmp.CapabilityType;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.ProducerFlowProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.RollbackException;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import com.solacesystems.jcsmp.transaction.TransactionResultUnknownException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.wso2.carbon.inbound.solace.DeliveryProcessor.Decision;
import org.wso2.carbon.inbound.solace.hooks.Hooks;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_DRAIN;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_INJECTION_REJECTED;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.CAUSE_TIMEOUT;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.SOLACE_INBOUND_SETTLEMENT;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.SOLACE_INBOUND_TX;
import static org.wso2.carbon.inbound.solace.SolaceInboundConstants.SOLACE_INBOUND_TX_PRODUCER;

/**
 * One transacted worker of a generation. The worker thread owns one transacted session,
 * one flow it reads synchronously and one producer. It is the only thread that touches the three.
 * The closer closes the published set through {@link #closeResources()}, and the drain calls the non
 * blocking {@code flow.stop()} through {@link TransactedBinding#stop()}. Nothing else reaches them.
 */
final class TxWorker implements Runnable {

    private static final Log log = LogFactory.getLog(TxWorker.class);

    final SolaceEventListener listener;
    final Generation g;
    final int index;
    private final String name;
    private final SolaceInboundConfig config;
    volatile ResourceSet current;
    private CompletableFuture<String> handle;
    private CommandQueue commands;
    private Generation.Delivery delivery;
    private boolean uncertain;
    private volatile Runnable running;

    TxWorker(SolaceEventListener listener, Generation generation, int index) {
        this.listener = listener;
        this.g = generation;
        this.index = index;
        this.name = listener.name;
        this.config = listener.config;
    }

    /**
     * The three JCSMP objects of one build. The builder owns them until it publishes the set, and
     * close() runs once. Closing the transacted session rolls back whatever is open.
     */
    static final class ResourceSet {
        TransactedSession ts;
        FlowReceiver flow;
        XMLMessageProducer producer;
        final AtomicBoolean closed = new AtomicBoolean();

        void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                if (flow != null) {
                    flow.close();
                }
            } catch (Exception e) {
                log.debug("transacted flow close failed", e);
            }
            try {
                if (producer != null) {
                    producer.close();
                }
            } catch (Exception e) {
                log.debug("transacted producer close failed", e);
            }
            try {
                if (ts != null) {
                    ts.close();
                }
            } catch (Exception e) {
                log.debug("transacted session close failed", e);
            }
        }
    }

    /**
     * Lifecycle thread (bind) or the worker thread itself (rebuild). Builds into a new set the builder
     * owns until it publishes it. Each field is assigned as soon as its resource exists, so a partial
     * build is released by the builder, never by the closer.
     */
    ResourceSet openResources() throws JCSMPException {
        ResourceSet resources = new ResourceSet();
        try {
            resources.ts = g.session.createTransactedSession();
            Queue endpoint = JCSMPFactory.onlyInstance().createQueue(config.destinationName);
            ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
            flowProperties.setEndpoint(endpoint);
            flowProperties.setStartState(false);
            if (StringUtils.isNotBlank(config.selector)) {
                flowProperties.setSelector(config.selector.trim());
            }
            if (g.session.isCapable(CapabilityType.ACTIVE_FLOW_INDICATION)) {
                flowProperties.setActiveFlowIndication(true);
            }
            Hooks.get().point("binding.flowProperties", flowProperties);
            // A null listener makes the flow synchronous. A transacted flow has no settlement outcomes.
            resources.flow = resources.ts.createFlow(null, flowProperties, null,
                    new SolaceFlowEventHandler(listener, g, index));
            // Requires the parent producer of the generation. The handler logs at DEBUG only.
            resources.producer = resources.ts.createProducer(new ProducerFlowProperties(),
                    new TxPublishEventHandler(listener, g, index));
        } catch (JCSMPException | RuntimeException e) {
            resources.close();
            throw e;
        }
        return resources;
    }

    /** At bind on the lifecycle thread before the barrier, or on a rebuild by the owner under the admission lock. */
    void publish(ResourceSet resources) {
        current = resources;
    }

    /** Closer thread, or the owner on a rebuild. Closes the published set only. An unpublished build stays its builder's. */
    void closeResources() {
        ResourceSet resources = current;
        if (resources != null) {
            resources.close();
        }
    }

    void drainRequested() {
        CommandQueue queue = commands;
        if (queue != null) {
            queue.wake();
        }
    }

    @Override
    public void run() {
        Counters counters = listener.counters;
        while (!g.retired && g.active) {
            BytesXMLMessage message;
            // Read once per step. The owner is the only writer.
            ResourceSet resources = current;
            try {
                message = resources.flow.receive(1000);
            } catch (JCSMPException e) {
                if (g.retired) {
                    return;
                }
                listener.terminalFailure(g, "transacted receive failed", e);
                return;
            }
            if (message == null) {
                continue;
            }
            final long receivedNanos = System.nanoTime();
            Hooks.get().point("txworker.afterReceive", g, message);
            counters.received.increment();
            if (message.getRedelivered()) {
                counters.redelivered.increment();
            }
            boolean admitted;
            synchronized (g.admission) {
                // Rechecked after the receive. No broker call under this lock.
                admitted = !g.draining && !g.retired;
                if (admitted || g.draining) {
                    // A draining, still live generation counts the rollback below as in flight.
                    g.inFlight++;
                    counters.gauge(g).inFlight.set(g.inFlight);
                    delivery = new Generation.Delivery(message, receivedNanos);
                    delivery.worker = Thread.currentThread();
                    g.inFlightDeliveries.add(delivery);
                }
            }
            if (g.retired) {
                // A retired generation makes no broker call. The closer's close rolls the delivery back.
                return;
            }
            if (!admitted) {
                complete(message, false, "not admitted during drain");
                releaseInFlight();
                return;
            }
            try {
                handleOneTransacted(message);
            } catch (Exception e) {
                log.error("SolaceListener [" + name + "] unexpected failure while handling transacted message "
                        + message.getMessageId() + "; rolling back", e);
                counters.handlerFailed.increment();
                complete(message, false, "unexpected failure");
            } catch (Error e) {
                // Accounting, then the generation recovers. A dead owner is a lost flow.
                counters.handlerFailed.increment();
                listener.terminalFailure(g, "transacted worker died", null);
                throw e;
            } finally {
                CommandQueue queue = commands;
                if (queue != null) {
                    queue.close();
                }
                releaseInFlight();
            }
        }
    }

    private void releaseInFlight() {
        synchronized (g.admission) {
            if (delivery != null) {
                g.inFlightDeliveries.remove(delivery);
                delivery = null;
            }
            g.inFlight--;
            listener.counters.gauge(g).inFlight.set(g.inFlight);
            if (g.inFlight == 0) {
                g.admission.notifyAll();
            }
        }
    }

    private void handleOneTransacted(BytesXMLMessage message) {
        Counters counters = listener.counters;
        int attempt = 1;
        if (message.isDeliveryCountSupported()) {
            try {
                attempt = Math.max(1, message.getDeliveryCount());
            } catch (UnsupportedOperationException e) {
                attempt = 1;
            }
        } else if (listener.loggedDeliveryCountOff.compareAndSet(false, true)) {
            log.info("SolaceListener [" + name + "] the queue has deliveryCountEnabled=false; transacted back-off uses "
                    + "the constant initial delay");
        }
        uncertain = false;
        delivery.completionNanos = 0;
        CompletableFuture<String> handle = new CompletableFuture<>();
        CommandQueue commands = new CommandQueue(Thread.currentThread(), this::runCommand);
        this.handle = handle;
        this.commands = commands;
        // Completion wakes the owner.
        handle.whenComplete((result, failure) -> commands.wake());
        synchronized (g.admission) {
            g.inFlightHandles.put(handle, delivery);
        }
        String value;
        try {
            MessageContext messageContext = null;
            boolean rejected;
            try {
                // One attempt per delivery, so the message is read once here and the context built from it.
                SolaceUtils.Payload payload = listener.injectHandler.readPayload(message);
                messageContext = listener.injectHandler.buildContext(payload, message, attempt);
            } catch (Exception e) {
                log.error("SolaceListener [" + name + "] message " + message.getMessageId()
                        + " could not be converted into a message context; rejecting", e);
            }
            rejected = (messageContext == null);
            if (rejected) {
                value = handle.complete(SettlementValues.record(CAUSE_INJECTION_REJECTED))
                        ? CAUSE_INJECTION_REJECTED : SettlementValues.cause(handle.getNow(null));
            } else {
                // The measurement record exists before the handle is exposed. A rejected injection never creates one.
                Hooks.get().point("metrics.handleCreated", handle);
                messageContext.setProperty(SOLACE_INBOUND_SETTLEMENT, handle);
                messageContext.setProperty(SOLACE_INBOUND_TX, commands);
                messageContext.setProperty(SOLACE_INBOUND_TX_PRODUCER, current.producer);
                Hooks.get().point("worker.beforeInject", g, handle);
                listener.injectHandler.inject(messageContext);
                // One absolute deadline per attempt. Tokens and commands never extend it.
                long deadline = System.currentTimeMillis() + config.settlementTimeoutMillis;
                value = awaitServicingCommands(handle, commands, deadline, message);
            }
        } finally {
            synchronized (g.admission) {
                g.inFlightHandles.remove(handle);
            }
        }
        counters.decided(value).increment();
        Decision decision = DeliveryProcessor.decide(value, config, name);
        commands.close();
        if (decision == Decision.ACK && !uncertain) {
            counters.applied(Decision.ACK).increment();
            complete(message, true, "ACK");
            return;
        }
        if (decision == Decision.ACK) {
            log.error("SolaceListener [" + name + "] message " + message.getMessageId()
                    + " decided ACK but the transaction is uncertain; rolling back");
            counters.applied(Decision.FAILED).increment();
            complete(message, false, "uncertain");
            return;
        }
        // FAILED, REJECTED, the TIMEOUT mapping and a rejected injection all roll back.
        counters.applied(decision == Decision.NONE ? Decision.FAILED : decision).increment();
        boolean drainWon = CAUSE_DRAIN.equals(value) || g.draining;
        if (!drainWon) {
            long delay = DeliveryProcessor.backoff(config.redeliveryInitialDelayMillis, config.redeliveryProgression,
                    config.redeliveryMaxDelayMillis, attempt);
            log.warn("SolaceListener [" + name + "] message " + message.getMessageId() + " (delivery " + attempt
                    + ") rolls back in " + delay + " ms: " + value);
            // The drain interrupts this sleep, and the rollback then follows at once. DRAIN never sleeps.
            DeliveryProcessor.sleepInterruptibly(g, delay);
        }
        complete(message, false, value);
    }

    private String awaitServicingCommands(CompletableFuture<String> handle, CommandQueue commands, long deadline,
                                          BytesXMLMessage message) {
        while (true) {
            if (handle.isDone()) {
                // Checked before blocking. A synchronous ACK commits at once.
                takeCompletionInstant(handle);
                return SettlementValues.cause(handle.getNow(null));
            }
            long now = System.currentTimeMillis();
            if (now >= deadline) {
                return timeoutWins(handle, message);
            }
            Object item;
            try {
                item = commands.poll(deadline - now);
            } catch (InterruptedException e) {
                // Only the drain interrupts, and only a sleeping worker. Re evaluate.
                Thread.interrupted();
                continue;
            }
            if (item == null) {
                return timeoutWins(handle, message);
            }
            Hooks.get().point("txworker.afterDequeue", g, item);
            if (System.currentTimeMillis() >= deadline) {
                // An item dequeued at the deadline never starts. poll() removed it, so it is completed here.
                if (item instanceof Future) {
                    reason(item, "TRANSACTION_COMPLETED");
                    ((Future<?>) item).cancel(false);
                }
                return timeoutWins(handle, message);
            }
            runCommand(item);
        }
    }

    /** The 7.2 winner rule. */
    private String timeoutWins(CompletableFuture<String> handle, BytesXMLMessage message) {
        boolean won = handle.complete(SettlementValues.record(CAUSE_TIMEOUT));
        if (won) {
            log.warn("SolaceListener [" + name + "] message " + message.getMessageId() + " not settled within "
                    + listener.config.settlementTimeoutMillis + " ms; rolling back");
        }
        // The measurement record leaves on every decision path. A lost race reads the winner's instant.
        if (won) {
            Hooks.get().point("metrics.handleDiscard", handle);
        } else {
            takeCompletionInstant(handle);
        }
        return won ? CAUSE_TIMEOUT : SettlementValues.cause(handle.getNow(null));
    }

    /** Owner thread, once per decided handle. The shipped build pays one no op point. */
    private void takeCompletionInstant(CompletableFuture<String> handle) {
        String value = handle.getNow(null);
        if (SettlementValues.isInboundRecord(value)) {
            // TIMEOUT, DRAIN, AUTOACK: an inbound completion. No commit follows, so the record is removed without waiting.
            Hooks.get().point("metrics.handleDiscard", handle);
            delivery.completionNanos = 0;
            return;
        }
        // A connector completion. The test build returns the winner's instant and removes the record.
        Object instant = Hooks.get().point("metrics.completionNanos", handle);
        // -1 means missing or ambiguous, and the commit sample is invalid.
        delivery.completionNanos = instant instanceof Long ? (Long) instant : -1L;
    }

    /** Owner thread. The procedure for every command, queued or inline. */
    private void runCommand(Object item) {
        if (!(item instanceof Runnable)) {
            // WAKE: nothing to run. The loop re evaluates the handle and the deadline.
            return;
        }
        Runnable command = (Runnable) item;
        Future<?> future = command instanceof Future ? (Future<?>) command : null;
        if (future != null && future.isCancelled()) {
            // CANCELLED by its caller, so it never runs.
            return;
        }
        if (future != null && command instanceof Delayed
                && ((Delayed) command).getDelay(TimeUnit.MILLISECONDS) <= 0) {
            // EXPIRED: the owner enforces the command's deadline and the caller reports DEADLINE.
            reason(command, "DEADLINE");
            future.cancel(false);
            return;
        }
        if (handle.isDone()) {
            // The decision won. Nothing runs after it.
            if (future != null) {
                reason(command, "TRANSACTION_COMPLETED");
                future.cancel(false);
            }
            return;
        }
        running = command;
        try {
            command.run();
        } finally {
            running = null;
        }
        if (future != null && future.isCancelled()) {
            // The caller stopped waiting while the command ran, so its outcome is unknown to it.
            // A cancel between the check above and FutureTask.run() lands here too, a spurious rollback in the safe direction.
            uncertain = true;
            log.error("SolaceListener [" + name + "] message " + delivery.messageId
                    + ": a connector command was cut off while running; the transaction is uncertain and will be "
                    + "rolled back");
        }
    }

    /** The owner stores its cancellation reason before cancel(false), so the caller reads it with the cancellation. */
    @SuppressWarnings("unchecked")
    static void reason(Object command, String cancellationReason) {
        if (command instanceof Consumer) {
            ((Consumer<String>) command).accept(cancellationReason);
        }
    }

    /** The completion of one delivery, commit or rollback, owner thread. */
    private void complete(BytesXMLMessage message, boolean commit, String why) {
        if (g.retired) {
            // The drain counted the delivery once. This line counts nothing.
            log.warn("SolaceListener [" + name + "] message " + message.getMessageId() + ": generation " + g.id
                    + " retired, the closer rolls the transaction back");
            return;
        }
        Counters counters = listener.counters;
        try {
            Hooks.get().point("txworker.beforeCommit", g, commit);
            if (commit) {
                // Measured for commits only, with the completing side's instant, recorded after the hook so an injected delay is inside the duration.
                if (delivery.completionNanos > 0) {
                    Hooks.get().record("tx.completionToCommitStart", System.nanoTime() - delivery.completionNanos);
                } else {
                    Hooks.get().record("tx.completionToCommitStart.invalid", 0);
                }
            }
            TransactedSession ts = current.ts;
            if (delivery != null) {
                Hooks.get().record("delivery.receiptToSettlementCall", System.nanoTime() - delivery.receivedNanos);
            }
            if (commit) {
                ts.commit();
                counters.committed.increment();
                if (log.isDebugEnabled()) {
                    log.debug("SolaceListener [" + name + "] message " + message.getMessageId() + " committed on "
                            + Thread.currentThread().getName());
                }
            } else {
                ts.rollback();
                counters.rolledBack.increment();
                log.warn("SolaceListener [" + name + "] message " + message.getMessageId() + " rolled back (" + why
                        + "); the broker redelivers it");
            }
            if (delivery != null) {
                Hooks.get().record("delivery.receiptToBrokerReturn", System.nanoTime() - delivery.receivedNanos);
            }
        } catch (RollbackException e) {
            // The broker rolled back and the session is kept. A commit the broker keeps refusing would loop without
            // the redelivery back off.
            counters.rolledBackByBroker.increment();
            log.warn("SolaceListener [" + name + "] message " + message.getMessageId()
                    + ": the broker rolled the transaction back; it will be redelivered", e);
            if (!g.draining) {
                int attempt = 1;
                if (message.isDeliveryCountSupported()) {
                    try {
                        attempt = Math.max(1, message.getDeliveryCount());
                    } catch (UnsupportedOperationException unsupported) {
                        attempt = 1;
                    }
                }
                long delay = DeliveryProcessor.backoff(config.redeliveryInitialDelayMillis,
                        config.redeliveryProgression, config.redeliveryMaxDelayMillis, attempt);
                log.info("SolaceListener [" + name + "] next receive after " + delay + " ms (broker rollback back off)");
                DeliveryProcessor.sleepInterruptibly(g, delay);
            }
        } catch (TransactionResultUnknownException e) {
            counters.unknown.increment();
            log.error("SolaceListener [" + name + "] message " + message.getMessageId()
                    + ": transaction result unknown; rebuilding the transacted session, flow and producer", e);
            rebuild();
        } catch (JCSMPException e) {
            // The rollback threw, or the commit threw anything else.
            counters.unknown.increment();
            log.error("SolaceListener [" + name + "] message " + message.getMessageId() + ": "
                    + (commit ? "commit" : "rollback")
                    + " failed; rebuilding the transacted session, flow and producer", e);
            rebuild();
        }
    }

    /** Owner thread, before the next receive. */
    private void rebuild() {
        // The published set is closed by its owner. A later closeResources() finds it closed.
        ResourceSet previous = current;
        if (previous != null) {
            previous.close();
        }
        if (g.retired || g.draining) {
            return;
        }
        ResourceSet fresh;
        try {
            // May block. The admission lock is not held. A failed build was closed by openResources itself.
            fresh = openResources();
        } catch (JCSMPException e) {
            listener.terminalFailure(g, "transacted session rebuild failed", e);
            return;
        }
        boolean start;
        synchronized (g.admission) {
            // The hand off is ordered with the drain's stop by the admission lock.
            start = !g.draining && !g.retired;
            if (start) {
                // Published first, so a retire that follows closes it through closeResources().
                publish(fresh);
                try {
                    fresh.flow.start();
                } catch (JCSMPException e) {
                    start = false;
                }
            }
        }
        if (!start) {
            // A build that loses is closed once, by its builder. close() is idempotent either way.
            fresh.close();
            log.info("SolaceListener [" + name + "] transacted rebuild of worker " + index + " discarded: generation "
                    + g.id + " is draining or retired");
            if (!g.retired && !g.draining) {
                listener.terminalFailure(g, "transacted flow restart failed", null);
            }
        }
    }
}
