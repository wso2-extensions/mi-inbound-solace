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

import org.apache.axis2.AxisFault;
import org.apache.axis2.builder.ApplicationXMLBuilder;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.format.BinaryBuilder;
import org.apache.axis2.format.PlainTextBuilder;
import org.apache.axis2.json.JSONBadgerfishMessageFormatter;
import org.apache.axis2.json.JSONBadgerfishOMBuilder;
import org.apache.axis2.json.JSONBuilder;
import org.apache.axis2.json.JSONMessageFormatter;
import org.apache.synapse.commons.builders.XFormURLEncodedBuilder;
import org.apache.synapse.commons.json.JsonStreamBuilder;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.apache.synapse.transport.passthru.util.BinaryRelayBuilder;

/**
 * Synapse environments for the unit tests. One registers the builders a WSO2 Integrator: MI pack registers for
 * the family types and for the legacy JSON types, with the formatters of the legacy JSON types. The other
 * registers none.
 */
final class TestEnvironments {

    private TestEnvironments() {
    }

    static SynapseEnvironment withPackBuilders() throws AxisFault {
        AxisConfiguration configuration = new AxisConfiguration();
        configuration.addMessageBuilder("application/xml", new ApplicationXMLBuilder());
        configuration.addMessageBuilder("application/json", new JsonStreamBuilder());
        configuration.addMessageBuilder("text/plain", new PlainTextBuilder());
        configuration.addMessageBuilder("application/octet-stream", new BinaryRelayBuilder());
        configuration.addMessageBuilder("application/binary", new BinaryBuilder());
        configuration.addMessageBuilder("application/x-www-form-urlencoded", new XFormURLEncodedBuilder());
        configuration.addMessageBuilder("application/json/badgerfish", new JSONBadgerfishOMBuilder());
        configuration.addMessageBuilder("text/javascript", new JSONBuilder());
        configuration.addMessageFormatter("application/json/badgerfish", new JSONBadgerfishMessageFormatter());
        configuration.addMessageFormatter("text/javascript", new JSONMessageFormatter());
        return new Axis2SynapseEnvironment(new ConfigurationContext(configuration), new SynapseConfiguration());
    }

    static SynapseEnvironment withoutBuilders() throws AxisFault {
        return new Axis2SynapseEnvironment(new ConfigurationContext(new AxisConfiguration()),
                new SynapseConfiguration());
    }
}
