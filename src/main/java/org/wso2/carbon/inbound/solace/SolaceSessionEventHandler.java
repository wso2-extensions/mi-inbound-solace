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

import com.solacesystems.jcsmp.SessionEvent;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Logs JCSMP session lifecycle events (RECONNECTING, RECONNECTED, DOWN_ERROR, ...) so operators
 * can observe reconnect activity, and drives listener teardown on terminal session failure.
 *
 * <p>{@code RECONNECTING}/{@code RECONNECTED} are transient — JCSMP is recovering the connection
 * on its own — so they are logged only. {@code DOWN_ERROR} is terminal (the session has failed and
 * will not reconnect); on that event the supplied {@code onSessionDown} callback is invoked to tear
 * the listener down so the MI framework can trigger a resume. This complements the
 * {@code XMLMessageListener.onException} path: onException is JCSMP's documented signal for
 * permanent connection loss, but it is not guaranteed to fire for every terminal session-down case,
 * so reacting to DOWN_ERROR as well closes that gap. The listener's {@code destroy()} is idempotent,
 * so if both paths fire the second teardown is a harmless no-op.
 */
public class SolaceSessionEventHandler implements SessionEventHandler {

    private static final Log log = LogFactory.getLog(SolaceSessionEventHandler.class);

    private final String listenerName;
    private final Runnable onSessionDown;

    public SolaceSessionEventHandler(String listenerName, Runnable onSessionDown) {
        this.listenerName = listenerName;
        this.onSessionDown = onSessionDown;
    }

    @Override
    public void handleEvent(SessionEventArgs eventArgs) {
        SessionEvent event = eventArgs.getEvent();
        if (event == SessionEvent.RECONNECTING) {
            log.warn("SolaceListener [" + listenerName + "] session reconnecting: " + eventArgs);
        } else if (event == SessionEvent.RECONNECTED) {
            log.info("SolaceListener [" + listenerName + "] session reconnected: " + eventArgs);
        } else if (event == SessionEvent.DOWN_ERROR) {
            log.error("SolaceListener [" + listenerName + "] session DOWN (terminal): "
                    + eventArgs + ". Tearing down listener for MI resume cycle.");
            // handleEvent runs on a JCSMP callback thread. onSessionDown is the listener's
            // destroy(), which calls session.closeSession() — closing the session from within
            // its own callback thread can deadlock, so run the teardown on a daemon thread.
            Thread teardown = new Thread(() -> {
                try {
                    onSessionDown.run();
                } catch (Exception e) {
                    log.error("Error while handling session DOWN_ERROR for listener ["
                            + listenerName + "]", e);
                }
            }, "solace-sessiondown-" + listenerName);
            teardown.setDaemon(true);
            teardown.start();
        } else if (log.isDebugEnabled()) {
            log.debug("SolaceListener [" + listenerName + "] session event: " + eventArgs);
        }
    }
}
