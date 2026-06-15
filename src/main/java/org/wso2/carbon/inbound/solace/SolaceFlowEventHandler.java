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

import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Logs JCSMP flow lifecycle events and reacts to terminal flow states.
 *
 * <p>Event handling:
 * <ul>
 *   <li>{@code FLOW_UP} — flow successfully bound; logged at INFO.</li>
 *   <li>{@code FLOW_ACTIVE} / {@code FLOW_INACTIVE} — for non-exclusive queues,
 *       signals whether this consumer is the active receiver; logged at INFO
 *       so operators can diagnose "why is my node silent?".</li>
 *   <li>{@code FLOW_RECONNECTING} / {@code FLOW_RECONNECTED} — transient flow
 *       reconnect (distinct from session-level reconnect); logged at WARN/INFO.</li>
 *   <li>{@code FLOW_DOWN} — terminal (queue deleted, permission revoked,
 *       broker rejected the bind). The supplied {@code onFlowDown} callback is
 *       invoked on a separate daemon thread so the listener can tear itself down
 *       (closing the flow/session off the JCSMP callback thread) and let the MI
 *       framework trigger a resume.</li>
 * </ul>
 */
public class SolaceFlowEventHandler implements FlowEventHandler {

    private static final Log log = LogFactory.getLog(SolaceFlowEventHandler.class);

    private final String listenerName;
    private final Runnable onFlowDown;

    public SolaceFlowEventHandler(String listenerName, Runnable onFlowDown) {
        this.listenerName = listenerName;
        this.onFlowDown = onFlowDown;
    }

    @Override
    public void handleEvent(Object source, FlowEventArgs eventArgs) {
        FlowEvent event = eventArgs.getEvent();
        if (event == FlowEvent.FLOW_UP) {
            log.info("SolaceListener [" + listenerName + "] flow UP: " + eventArgs);
        } else if (event == FlowEvent.FLOW_ACTIVE) {
            log.info("SolaceListener [" + listenerName + "] flow ACTIVE "
                    + "(now consuming on non-exclusive queue): " + eventArgs);
        } else if (event == FlowEvent.FLOW_INACTIVE) {
            log.info("SolaceListener [" + listenerName + "] flow INACTIVE "
                    + "(not active consumer on non-exclusive queue): " + eventArgs);
        } else if (event == FlowEvent.FLOW_RECONNECTING) {
            log.warn("SolaceListener [" + listenerName + "] flow reconnecting: " + eventArgs);
        } else if (event == FlowEvent.FLOW_RECONNECTED) {
            log.info("SolaceListener [" + listenerName + "] flow reconnected: " + eventArgs);
        } else if (event == FlowEvent.FLOW_DOWN) {
            log.error("SolaceListener [" + listenerName + "] flow DOWN (terminal): "
                    + eventArgs + ". Tearing down listener for MI resume cycle.");
            // Run on a daemon thread: closing a flow/session from within its own JCSMP
            // callback thread can block.
            Thread teardown = new Thread(() -> {
                try {
                    // onFlowDown is the listener's destroy(): closes the flow/session and
                    // clears isConnected so the MI framework can resume the listener.
                    onFlowDown.run();
                } catch (Exception e) {
                    log.error("Error while handling FLOW_DOWN for listener ["
                            + listenerName + "]", e);
                }
            }, "solace-flowdown-" + listenerName);
            teardown.setDaemon(true);
            teardown.start();
        } else if (log.isDebugEnabled()) {
            log.debug("SolaceListener [" + listenerName + "] flow event: " + eventArgs);
        }
    }
}
