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

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axiom.soap.SOAPFactory;
import org.apache.axis2.context.MessageContext;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.SynapseEnvironment;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SolaceUtils {

    private static final Log log = LogFactory.getLog(SolaceUtils.class);

    private SolaceUtils() {
        // Utility class
    }

    /**
     * Extracts the message payload as a String from all supported Solace message subtypes.
     *
     * @param message               the Solace message
     * @param binaryPayloadAsBase64 when true, BytesMessage raw bytes are base64-encoded
     *                              instead of UTF-8 decoded (prevents corruption of binary data)
     */
    public static String extractPayload(BytesXMLMessage message, boolean binaryPayloadAsBase64) {
        if (message instanceof TextMessage) {
            return ((TextMessage) message).getText();
        }
        if (message instanceof BytesMessage) {
            byte[] data = ((BytesMessage) message).getData();
            if (data != null) {
                if (binaryPayloadAsBase64) {
                    return Base64.getEncoder().encodeToString(data);
                }
                return new String(data, StandardCharsets.UTF_8);
            }
        }
        if (message instanceof XMLContentMessage) {
            return ((XMLContentMessage) message).getXMLContent();
        }
        if (message instanceof MapMessage) {
            SDTMap map = ((MapMessage) message).getMap();
            if (map != null) {
                return sdtMapToJson(map);
            }
        }
        if (message instanceof StreamMessage) {
            SDTStream stream = ((StreamMessage) message).getStream();
            if (stream != null) {
                return sdtStreamToJson(stream);
            }
        }
        if (message.getAttachmentByteBuffer() != null) {
            byte[] bytes = new byte[message.getAttachmentByteBuffer().remaining()];
            message.getAttachmentByteBuffer().get(bytes);
            if (binaryPayloadAsBase64) {
                return Base64.getEncoder().encodeToString(bytes);
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return null;
    }

    /**
     * Creates a Synapse MessageContext populated with the message payload.
     */
    public static org.apache.synapse.MessageContext createMessageContext(
            SynapseEnvironment synapseEnvironment,
            String payload,
            BytesXMLMessage message,
            String contentType,
            boolean binaryPayloadAsBase64) throws Exception {

        org.apache.synapse.MessageContext synCtx = synapseEnvironment.createMessageContext();
        MessageContext axis2MsgCtx = ((org.apache.synapse.core.axis2.Axis2MessageContext) synCtx)
                .getAxis2MessageContext();

        String resolvedContentType;
        if (binaryPayloadAsBase64 && (message instanceof BytesMessage
                || !(message instanceof TextMessage
                        || message instanceof XMLContentMessage
                        || message instanceof MapMessage
                        || message instanceof StreamMessage))) {
            resolvedContentType = SolaceInboundConstants.CONTENT_TYPE_OCTET;
        } else {
            resolvedContentType = resolveContentType(payload, message, contentType);
        }

        axis2MsgCtx.setProperty(
                org.apache.axis2.Constants.Configuration.CONTENT_TYPE, resolvedContentType);
        axis2MsgCtx.setProperty(
                org.apache.axis2.Constants.Configuration.MESSAGE_TYPE, resolvedContentType);

        Map<String, Object> transportHeaders = new HashMap<>();
        transportHeaders.put("Content-Type", resolvedContentType);
        axis2MsgCtx.setProperty(
                org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS, transportHeaders);

        if (SolaceInboundConstants.CONTENT_TYPE_JSON.equalsIgnoreCase(resolvedContentType)) {
            try {
                InputStream in = new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8));
                // addAsNewFirstChild=true so the envelope body gets a <jsonObject>/<jsonArray>
                // wrapper — otherwise SynapseJsonPath/LogMediator cannot locate the payload.
                JsonUtil.getNewJsonPayload(axis2MsgCtx, in, true, true);
            } catch (Exception e) {
                log.warn("Failed to parse JSON payload, falling back to text/plain.", e);
                axis2MsgCtx.setProperty(
                        org.apache.axis2.Constants.Configuration.CONTENT_TYPE,
                        SolaceInboundConstants.CONTENT_TYPE_TEXT);
                axis2MsgCtx.setProperty(
                        org.apache.axis2.Constants.Configuration.MESSAGE_TYPE,
                        SolaceInboundConstants.CONTENT_TYPE_TEXT);
                SOAPFactory soapFactory = OMAbstractFactory.getSOAP12Factory();
                SOAPEnvelope envelope = soapFactory.getDefaultEnvelope();
                addTextPayload(envelope, payload);
                synCtx.setEnvelope(envelope);
            }
        } else if (SolaceInboundConstants.CONTENT_TYPE_XML.equalsIgnoreCase(resolvedContentType)
                || SolaceInboundConstants.CONTENT_TYPE_TEXT_XML.equalsIgnoreCase(resolvedContentType)) {
            SOAPFactory soapFactory = OMAbstractFactory.getSOAP12Factory();
            SOAPEnvelope envelope = soapFactory.getDefaultEnvelope();
            try {
                OMElement element = AXIOMUtil.stringToOM(payload);
                envelope.getBody().addChild(element);
            } catch (Exception e) {
                log.warn("Failed to parse XML payload, falling back to text/plain.", e);
                axis2MsgCtx.setProperty(
                        org.apache.axis2.Constants.Configuration.CONTENT_TYPE,
                        SolaceInboundConstants.CONTENT_TYPE_TEXT);
                axis2MsgCtx.setProperty(
                        org.apache.axis2.Constants.Configuration.MESSAGE_TYPE,
                        SolaceInboundConstants.CONTENT_TYPE_TEXT);
                addTextPayload(envelope, payload);
            }
            synCtx.setEnvelope(envelope);
        } else {
            SOAPFactory soapFactory = OMAbstractFactory.getSOAP12Factory();
            SOAPEnvelope envelope = soapFactory.getDefaultEnvelope();
            addTextPayload(envelope, payload);
            synCtx.setEnvelope(envelope);
        }
        return synCtx;
    }

    private static void addTextPayload(SOAPEnvelope envelope, String payload) {
        OMFactory omFactory = OMAbstractFactory.getOMFactory();
        OMNamespace ns = omFactory.createOMNamespace(
                "http://ws.apache.org/commons/ns/payload", "text");
        OMElement textElement = omFactory.createOMElement("message", ns);
        textElement.setText(payload);
        envelope.getBody().addChild(textElement);
    }

    private static String resolveContentType(String payload, BytesXMLMessage message,
                                             String contentType) {
        // 1. Explicit inbound param override (highest priority)
        if (contentType != null) {
            return contentType;
        }
        // 2. HTTP Content-Type set by the publisher (the authoritative signal)
        String solaceContentType = message.getHTTPContentType();
        if (StringUtils.isNotEmpty(solaceContentType)) {
            return solaceContentType;
        }
        // 3. JCSMP message type — XMLContentMessage is a strong XML hint regardless of payload shape
        if (message instanceof XMLContentMessage) {
            return SolaceInboundConstants.CONTENT_TYPE_XML;
        }
        // 4. Last-resort payload sniff
        return detectContentTypeFromPayload(payload);
    }

    private static String detectContentTypeFromPayload(String payload) {
        if (payload == null || payload.isEmpty()) {
            return SolaceInboundConstants.DEFAULT_CONTENT_TYPE;
        }
        String trimmed = payload.trim();
        if ((trimmed.startsWith("{") && trimmed.endsWith("}"))
                || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            return SolaceInboundConstants.CONTENT_TYPE_JSON;
        }
        if (trimmed.startsWith("<")) {
            return SolaceInboundConstants.CONTENT_TYPE_XML;
        }
        return SolaceInboundConstants.CONTENT_TYPE_TEXT;
    }

    private static String sdtMapToJson(SDTMap map) {
        JSONObject json = new JSONObject();
        for (String key : map.keySet()) {
            try {
                Object value = map.get(key);
                if (value instanceof SDTMap) {
                    json.put(key, new JSONObject(sdtMapToJson((SDTMap) value)));
                } else if (value instanceof SDTStream) {
                    json.put(key, new JSONArray(sdtStreamToJson((SDTStream) value)));
                } else {
                    json.put(key, value);
                }
            } catch (SDTException e) {
                log.warn("Skipping unreadable SDTMap entry '" + key + "'", e);
            }
        }
        return json.toString();
    }

    private static String sdtStreamToJson(SDTStream stream) {
        JSONArray array = new JSONArray();
        while (stream.hasRemaining()) {
            try {
                Object value = stream.read();
                if (value instanceof SDTMap) {
                    array.put(new JSONObject(sdtMapToJson((SDTMap) value)));
                } else if (value instanceof SDTStream) {
                    array.put(new JSONArray(sdtStreamToJson((SDTStream) value)));
                } else {
                    array.put(value);
                }
            } catch (SDTException e) {
                log.warn("Skipping unreadable SDTStream entry", e);
                break;
            }
        }
        return array.toString();
    }

    public static void validateRequiredParam(String value, String paramName, String context) {
        if (StringUtils.isEmpty(value)) {
            throw new SynapseException(paramName + " is required for " + context);
        }
    }

    public static int getIntProperty(Properties properties, String key, int defaultValue) {
        return NumberUtils.toInt(properties.getProperty(key), defaultValue);
    }

    public static boolean getBooleanProperty(Properties properties, String key,
                                             boolean defaultValue) {
        return BooleanUtils.toBooleanDefaultIfNull(
                BooleanUtils.toBooleanObject(properties.getProperty(key)),
                defaultValue);
    }
}
