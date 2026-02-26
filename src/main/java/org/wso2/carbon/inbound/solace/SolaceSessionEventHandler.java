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
 * Logs JCSMP session lifecycle events (RECONNECTING, RECONNECTED_OK, DOWN_ERROR, ...)
 * so operators can observe reconnect activity without depending on {@code onException}
 * callbacks, which fire only after JCSMP's internal reconnect retries are exhausted.
 */
public class SolaceSessionEventHandler implements SessionEventHandler {

    private static final Log log = LogFactory.getLog(SolaceSessionEventHandler.class);

    private final String listenerName;

    public SolaceSessionEventHandler(String listenerName) {
        this.listenerName = listenerName;
    }

    @Override
    public void handleEvent(SessionEventArgs eventArgs) {
        SessionEvent event = eventArgs.getEvent();
        if (event == SessionEvent.RECONNECTING) {
            log.warn("SolaceListener [" + listenerName + "] session reconnecting: " + eventArgs);
        } else if (event == SessionEvent.RECONNECTED) {
            log.info("SolaceListener [" + listenerName + "] session reconnected: " + eventArgs);
        } else if (event == SessionEvent.DOWN_ERROR) {
            log.error("SolaceListener [" + listenerName + "] session DOWN: " + eventArgs);
        } else if (log.isDebugEnabled()) {
            log.debug("SolaceListener [" + listenerName + "] session event: " + eventArgs);
        }
    }
}
