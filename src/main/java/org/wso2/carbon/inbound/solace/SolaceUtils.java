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

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.MalformedJsonException;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;

import org.apache.axiom.attachments.ByteArrayDataSource;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.format.DataSourceMessageBuilder;
import org.apache.axis2.json.AbstractJSONOMBuilder;
import org.apache.axis2.transport.TransportUtils;
import org.apache.axis2.transport.http.HTTPConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class SolaceUtils {

    private static final Log log = LogFactory.getLog(SolaceUtils.class);

    private static final String CHARSET = "charset";

    /**
     * The stock builders that read characters: the plain text, XML and JSON builders. A subclass of one of
     * them reads characters too. We compare class names because the JSON builder of WSO2 Integrator: MI is
     * not on the compile path of the inbound.
     */
    private static final Set<String> CHARACTER_BUILDERS = Set.of(
            "org.apache.axis2.format.PlainTextBuilder",
            "org.apache.axis2.builder.ApplicationXMLBuilder",
            "org.apache.axis2.builder.SOAPBuilder",
            "org.apache.axis2.json.AbstractJSONOMBuilder",
            "org.apache.axis2.json.JSONBuilder",
            "org.apache.axis2.json.JSONStreamBuilder",
            "org.apache.axis2.json.gson.JsonBuilder",
            "org.apache.synapse.commons.json.JsonBuilder",
            "org.apache.synapse.commons.json.JsonStreamBuilder",
            "org.wso2.micro.integrator.core.json.JsonStreamBuilder");

    private SolaceUtils() {
        // Utility class
    }

    /**
     * The snapshot of one delivery. We read the JCSMP message once, before the first attempt, and every attempt
     * builds its own mediation context from these bytes. A stream or map message consumes its read cursor on the
     * first conversion, so a second read of the message would give an empty body.
     */
    public static final class Payload {

        /** The body. Canonical UTF-8 for text, the bytes as received for a binary body. Never null. */
        final byte[] bytes;

        /**
         * The content type the body is read with: the endpoint's contentType parameter with the inherited
         * charset, else the type the message declares, else the default of the message class.
         */
        final String contentType;

        /** True when the bytes are canonical UTF-8 text. */
        final boolean text;

        /** True when binaryPayloadAsBase64 made a byte body into base64 text, the legacy option. */
        final boolean base64;

        /** The builder key the body is built with. Null for the legacy base64 body. */
        final String messageType;

        /**
         * The builder registered for the message type, selected once for the delivery. Null when none is
         * registered, or for the legacy base64 body.
         */
        final Builder builder;

        /** The user properties as the sequence reads them, in the order of the message. */
        final Map<String, String> userProperties;

        Payload(byte[] bytes, String contentType, boolean text, boolean base64, String messageType, Builder builder,
                Map<String, String> userProperties) {
            this.bytes = bytes;
            this.contentType = contentType;
            this.text = text;
            this.base64 = base64;
            this.messageType = messageType;
            this.builder = builder;
            this.userProperties = userProperties;
        }

        boolean isEmpty() {
            return bytes.length == 0;
        }
    }

    /**
     * Reads the JCSMP message once and returns the snapshot of the delivery. The builder of the content type
     * is selected here, once for the delivery. Text a message carries as characters is held as canonical
     * UTF-8. A byte body of a character type is decoded with the charset of its content type and held as
     * canonical UTF-8. Any other byte body is held as the bytes it has.
     * Throws when a byte body does not decode with its charset, when the charset is unknown, when the text
     * has characters that UTF-8 cannot carry, or when a legacy JSON builder would get a body that is not one
     * complete JSON value. Nothing is ever replaced.
     *
     * @param synapseEnvironment    the environment whose registered builders build the body
     * @param message               the Solace message
     * @param contentTypeParameter  the endpoint's contentType parameter. Blank counts as absent.
     * @param binaryPayloadAsBase64 when true a byte body becomes base64 text, as 2.0.0 did
     * @param userProperties        the user properties of the message, already read
     */
    public static Payload readPayload(SynapseEnvironment synapseEnvironment, BytesXMLMessage message,
                                      String contentTypeParameter, boolean binaryPayloadAsBase64,
                                      Map<String, String> userProperties) throws IOException {
        String characters = null;
        byte[] raw = null;
        String classDefault;
        if (message instanceof TextMessage) {
            characters = ((TextMessage) message).getText();
            classDefault = SolaceInboundConstants.CONTENT_TYPE_TEXT;
        } else if (message instanceof BytesMessage) {
            raw = ((BytesMessage) message).getData();
            classDefault = SolaceInboundConstants.CONTENT_TYPE_OCTET;
        } else if (message instanceof XMLContentMessage) {
            characters = ((XMLContentMessage) message).getXMLContent();
            classDefault = SolaceInboundConstants.CONTENT_TYPE_XML;
        } else if (message instanceof MapMessage) {
            SDTMap map = ((MapMessage) message).getMap();
            characters = map == null ? null : sdtMapToJson(map);
            classDefault = SolaceInboundConstants.CONTENT_TYPE_JSON;
        } else if (message instanceof StreamMessage) {
            SDTStream stream = ((StreamMessage) message).getStream();
            characters = stream == null ? null : sdtStreamToJson(stream);
            classDefault = SolaceInboundConstants.CONTENT_TYPE_JSON;
        } else {
            // A message of no listed class carries its body as an attachment. It is bytes, never guessed.
            if (message.getAttachmentByteBuffer() != null) {
                raw = new byte[message.getAttachmentByteBuffer().remaining()];
                message.getAttachmentByteBuffer().get(raw);
            }
            classDefault = SolaceInboundConstants.CONTENT_TYPE_OCTET;
        }

        if (binaryPayloadAsBase64 && !isCharacterClass(message)) {
            byte[] base64 = raw == null ? new byte[0]
                    : Base64.getEncoder().encodeToString(raw).getBytes(StandardCharsets.US_ASCII);
            return new Payload(base64, SolaceInboundConstants.CONTENT_TYPE_OCTET, false, true, null, null,
                    userProperties);
        }

        String declared = StringUtils.isNotBlank(message.getHTTPContentType())
                ? message.getHTTPContentType().trim() : null;
        String resolved;
        if (StringUtils.isNotBlank(contentTypeParameter)) {
            resolved = withInheritedCharset(contentTypeParameter.trim(), declared);
        } else if (declared != null) {
            resolved = declared;
        } else {
            resolved = classDefault;
        }

        String baseType = baseType(resolved);
        MessageContext selection = new MessageContext();
        selection.setConfigurationContext(
                ((Axis2SynapseEnvironment) synapseEnvironment).getAxis2ConfigurationContext());
        String messageType;
        Builder builder;
        boolean form = HTTPConstants.MEDIA_TYPE_X_WWW_FORM.equals(baseType)
                || HTTPConstants.MEDIA_TYPE_MULTIPART_FORM_DATA.equals(baseType);
        if (form) {
            // A form is carried as an opaque binary body. No field is parsed.
            messageType = SolaceInboundConstants.CONTENT_TYPE_OCTET;
            builder = BuilderUtil.getBuilderFromSelector(messageType, selection);
        } else if (HTTPConstants.MEDIA_TYPE_TEXT_XML.equals(baseType)
                || HTTPConstants.MEDIA_TYPE_APPLICATION_SOAP_XML.equals(baseType)) {
            // The SOAP types are built by the XML builder. The received document goes into the body as it is.
            messageType = SolaceInboundConstants.CONTENT_TYPE_XML;
            builder = BuilderUtil.getBuilderFromSelector(messageType, selection);
        } else {
            messageType = baseType;
            builder = BuilderUtil.getBuilderFromSelector(messageType, selection);
            if (builder == null) {
                messageType = familyFallback(baseType);
                builder = BuilderUtil.getBuilderFromSelector(messageType, selection);
            }
        }

        // Text the message carries as characters is always canonical UTF-8. A byte body is decoded when its
        // type is a character type or its builder reads characters. Any other byte body is never converted.
        // A form is the exception. Its charset says how the percent encoded octets are read, so relabelling it
        // would change the value. The text of a form is encoded with its own charset and the declaration stays.
        boolean text = !form
                && (isCharacterClass(message) || isCharacterType(baseType) || isCharacterBuilder(builder));
        byte[] bytes;
        if (characters != null) {
            bytes = form ? encode(characters, charsetOf(resolved)) : encodeUtf8(characters);
        } else if (raw == null) {
            bytes = new byte[0];
        } else if (text) {
            bytes = encodeUtf8(decode(raw, charsetOf(resolved)));
        } else {
            bytes = raw;
        }
        if (builder instanceof AbstractJSONOMBuilder && bytes.length > 0) {
            checkCompleteJson(bytes, resolved);
        }
        return new Payload(bytes, resolved, text, false, messageType, builder, userProperties);
    }

    private static boolean isCharacterClass(BytesXMLMessage message) {
        return message instanceof TextMessage
                || message instanceof XMLContentMessage
                || message instanceof MapMessage
                || message instanceof StreamMessage;
    }

    /**
     * The endpoint parameter keeps its own charset. When it names none it takes the charset the message
     * declares. No other parameter of the declaration is inherited.
     */
    static String withInheritedCharset(String parameter, String declared) {
        if (charsetParameter(parameter) != null || declared == null) {
            return parameter;
        }
        String declaredCharset = charsetParameter(declared);
        if (declaredCharset == null) {
            return parameter;
        }
        return parameter + "; " + CHARSET + "=" + declaredCharset;
    }

    /**
     * A character type is a text/* type, text/javascript among them, a type of the JSON or the XML family,
     * or application/json/badgerfish.
     */
    static boolean isCharacterType(String baseType) {
        return baseType.startsWith("text/")
                || SolaceInboundConstants.CONTENT_TYPE_JSON.equals(baseType)
                || SolaceInboundConstants.CONTENT_TYPE_XML.equals(baseType)
                || baseType.endsWith("+json")
                || baseType.endsWith("+xml")
                || SolaceInboundConstants.CONTENT_TYPE_JSON_BADGERFISH.equals(baseType);
    }

    /**
     * True when the builder is one of the stock character builders or a subclass of one.
     */
    static boolean isCharacterBuilder(Builder builder) {
        for (Class<?> type = builder == null ? null : builder.getClass(); type != null;
             type = type.getSuperclass()) {
            if (CHARACTER_BUILDERS.contains(type.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The legacy JSON builders scan the body for the first property name and do not stop at the end of an
     * incomplete body. So before such a builder is called we parse the body once with a finite parse. It must
     * be one complete JSON value with nothing after it. A failure fails the build.
     */
    private static void checkCompleteJson(byte[] bytes, String contentType) throws AxisFault {
        try (JsonReader reader = new JsonReader(
                new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8))) {
            reader.skipValue();
            if (reader.peek() != JsonToken.END_DOCUMENT) {
                throw new MalformedJsonException("Content follows the JSON value");
            }
        } catch (IOException e) {
            throw new AxisFault("The body of content type " + contentType
                    + " is not one complete JSON value, so the legacy JSON builder is not called", e);
        }
    }

    /**
     * The builder key when no builder is registered for the base type: application/json for the JSON family,
     * application/xml for the XML family, text/plain for other text and application/octet-stream for the rest.
     * A missing builder never turns bytes into XML.
     */
    static String familyFallback(String baseType) {
        if (SolaceInboundConstants.CONTENT_TYPE_JSON.equals(baseType) || baseType.endsWith("+json")) {
            return SolaceInboundConstants.CONTENT_TYPE_JSON;
        }
        if (SolaceInboundConstants.CONTENT_TYPE_XML.equals(baseType)
                || SolaceInboundConstants.CONTENT_TYPE_TEXT_XML.equals(baseType)
                || baseType.endsWith("+xml")) {
            return SolaceInboundConstants.CONTENT_TYPE_XML;
        }
        if (baseType.startsWith("text/")) {
            return SolaceInboundConstants.CONTENT_TYPE_TEXT;
        }
        return SolaceInboundConstants.CONTENT_TYPE_OCTET;
    }

    /**
     * Decodes bytes with a decoder that reports malformed and unmappable input. A failure is never replaced.
     */
    private static String decode(byte[] bytes, Charset charset) throws CharacterCodingException {
        return charset.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
    }

    /**
     * Encodes text as UTF-8 with an encoder that reports what it cannot encode, such as a lone surrogate.
     */
    private static byte[] encodeUtf8(String text) throws CharacterCodingException {
        return encode(text, StandardCharsets.UTF_8);
    }

    /**
     * Encodes text with an encoder of the given charset that reports what it cannot encode. A failure is never
     * replaced.
     */
    private static byte[] encode(String text, Charset charset) throws CharacterCodingException {
        ByteBuffer buffer = charset.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .encode(CharBuffer.wrap(text));
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    /**
     * The charset a content type names, UTF-8 when it names none. An unknown name throws.
     */
    static Charset charsetOf(String contentType) {
        String name = charsetParameter(contentType);
        return name == null ? StandardCharsets.UTF_8 : Charset.forName(name);
    }

    /**
     * The base type of a content type, trimmed and in lower case.
     */
    static String baseType(String contentType) {
        return parts(contentType).get(0).trim().toLowerCase(Locale.ROOT);
    }

    /**
     * The value of the first charset parameter that has one, unquoted. The parameter name compares case
     * insensitively. Null when there is none.
     */
    static String charsetParameter(String contentType) {
        List<String> parts = parts(contentType);
        for (int i = 1; i < parts.size(); i++) {
            String parameter = parts.get(i);
            if (isCharset(parameter)) {
                String name = parameterValue(parameter).trim();
                if (!name.isEmpty()) {
                    return name;
                }
            }
        }
        return null;
    }

    /**
     * The declaration for canonical UTF-8 text: the declared base type, its other parameters and
     * charset=UTF-8. A content type never carries two charset parameters.
     */
    static String canonicalTextType(String contentType) {
        return withoutCharset(contentType) + "; " + CHARSET + "=" + StandardCharsets.UTF_8.name();
    }

    /**
     * The content type without its charset parameters. The base type and the other parameters stay as
     * written.
     */
    static String withoutCharset(String contentType) {
        List<String> parts = parts(contentType);
        StringBuilder type = new StringBuilder(parts.get(0).trim());
        for (int i = 1; i < parts.size(); i++) {
            String parameter = parts.get(i).trim();
            if (!parameter.isEmpty() && !isCharset(parameter)) {
                type.append("; ").append(parameter);
            }
        }
        return type.toString();
    }

    private static boolean isCharset(String parameter) {
        int eq = parameter.indexOf('=');
        return eq > 0 && CHARSET.equalsIgnoreCase(parameter.substring(0, eq).trim());
    }

    /**
     * The value of a parameter. A token is taken as written. A quoted string loses its quotes, and a
     * backslash inside it stands for the character after it.
     */
    private static String parameterValue(String parameter) {
        String value = parameter.substring(parameter.indexOf('=') + 1).trim();
        if (!value.startsWith("\"")) {
            return value;
        }
        StringBuilder unquoted = new StringBuilder();
        for (int i = 1; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' && i + 1 < value.length()) {
                unquoted.append(value.charAt(++i));
            } else if (c == '"') {
                break;
            } else {
                unquoted.append(c);
            }
        }
        return unquoted.toString();
    }

    /**
     * Splits a content type at the semicolons that separate its parameters. We follow the quoted string rules
     * of RFC 9110. Inside quotes a backslash escapes the next character, so an escaped quote does not end the
     * value and an escaped backslash does not escape what follows it. A semicolon inside quotes is part of
     * the value. Each part keeps its text as written. The first part is the base type.
     */
    private static List<String> parts(String contentType) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < contentType.length(); i++) {
            char c = contentType.charAt(i);
            if (quoted && c == '\\' && i + 1 < contentType.length()) {
                current.append(c).append(contentType.charAt(++i));
                continue;
            }
            if (c == '"') {
                quoted = !quoted;
            } else if (c == ';' && !quoted) {
                parts.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(c);
        }
        parts.add(current.toString());
        return parts;
    }

    /**
     * Creates a fresh Synapse MessageContext from the snapshot of the delivery. The body is built by the
     * builder the snapshot selected for its content type, the way the file inbound builds a file. Each call
     * gets its own reader over the snapshot, so no stream is shared between attempts.
     */
    public static org.apache.synapse.MessageContext createMessageContext(
            SynapseEnvironment synapseEnvironment, Payload payload) throws AxisFault {

        org.apache.synapse.MessageContext synCtx = synapseEnvironment.createMessageContext();
        MessageContext axis2MsgCtx = ((Axis2MessageContext) synCtx).getAxis2MessageContext();

        Map<String, Object> transportHeaders = new HashMap<>();
        axis2MsgCtx.setProperty(MessageContext.TRANSPORT_HEADERS, transportHeaders);

        if (payload.base64) {
            // The legacy option keeps the 2.0.0 body, the base64 text in the message element, as octet stream.
            applyContentType(axis2MsgCtx, transportHeaders,
                    SolaceInboundConstants.CONTENT_TYPE_OCTET, SolaceInboundConstants.CONTENT_TYPE_OCTET);
            SOAPEnvelope envelope = OMAbstractFactory.getSOAP12Factory().getDefaultEnvelope();
            addTextPayload(envelope, new String(payload.bytes, StandardCharsets.US_ASCII));
            synCtx.setEnvelope(envelope);
            return synCtx;
        }

        // The builder gets a declaration that matches the bytes. Text is canonical UTF-8. A binary body keeps
        // the type and the parameters it was read with.
        String contentType = payload.text ? canonicalTextType(payload.contentType) : payload.contentType;
        String charset = payload.text ? StandardCharsets.UTF_8.name() : charsetParameter(payload.contentType);
        if (charset != null) {
            axis2MsgCtx.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, charset);
        }

        String messageType = payload.messageType;
        Builder builder = payload.builder;
        if (builder == null) {
            throw new AxisFault("No message builder is registered for " + messageType
                    + ", the type used for the content type " + payload.contentType);
        }
        if (log.isDebugEnabled()) {
            log.debug("Solace message of content type " + payload.contentType + " is built by "
                    + builder.getClass().getName() + " with message type " + messageType);
        }

        OMElement documentElement;
        if (builder instanceof DataSourceMessageBuilder) {
            documentElement = ((DataSourceMessageBuilder) builder).processDocument(
                    new ByteArrayDataSource(payload.bytes, contentType), contentType, axis2MsgCtx);
        } else {
            documentElement = builder.processDocument(
                    new ByteArrayInputStream(payload.bytes), contentType, axis2MsgCtx);
        }
        synCtx.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));

        // The content type lives in three places and all three describe the built body. The charset is in
        // ContentType already. With setCharacterEncoding false the HTTP paths that read ContentType send it as
        // it is, with no second charset.
        axis2MsgCtx.setProperty(Constants.Configuration.MESSAGE_TYPE, messageType);
        axis2MsgCtx.setProperty(Constants.Configuration.CONTENT_TYPE, contentType);
        axis2MsgCtx.setProperty(Constants.Configuration.SET_CONTENT_TYPE_CHARACTER_ENCODING, "false");
        transportHeaders.keySet().removeIf(HTTPConstants.HEADER_CONTENT_TYPE::equalsIgnoreCase);
        transportHeaders.put(HTTPConstants.HEADER_CONTENT_TYPE, contentType);
        // The XML builder records the encoding the document declares. The bytes are canonical UTF-8 now, so a
        // declaration written later follows the charset of the format that writes it.
        axis2MsgCtx.removeProperty(Constants.Configuration.XML_DECLARATION_ENCODING);
        return synCtx;
    }

    /**
     * Sets the content type of the legacy base64 body in the axis2 ContentType and messageType properties
     * and in the transport Content-Type header, as 2.0.0 did.
     */
    private static void applyContentType(MessageContext axis2MsgCtx, Map<String, Object> transportHeaders,
                                         String contentType, String messageType) {
        axis2MsgCtx.setProperty(Constants.Configuration.CONTENT_TYPE, contentType);
        axis2MsgCtx.setProperty(Constants.Configuration.MESSAGE_TYPE, messageType);
        transportHeaders.put(HTTPConstants.HEADER_CONTENT_TYPE, contentType);
    }

    private static void addTextPayload(SOAPEnvelope envelope, String payload) {
        OMFactory omFactory = OMAbstractFactory.getOMFactory();
        OMNamespace ns = omFactory.createOMNamespace(
                "http://ws.apache.org/commons/ns/payload", "text");
        OMElement textElement = omFactory.createOMElement("message", ns);
        textElement.setText(payload);
        envelope.getBody().addChild(textElement);
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
        if (StringUtils.isBlank(value)) {
            throw new SynapseException(paramName + " is required for " + context);
        }
    }

    public static int parseInt(String value, String name, int defaultValue, List<String> errors) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            errors.add("invalid value '" + value + "' for " + name);
            return defaultValue;
        }
    }

    public static long parseLong(String value, String name, long defaultValue, List<String> errors) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            errors.add("invalid value '" + value + "' for " + name);
            return defaultValue;
        }
    }

    public static double parseDouble(String value, String name, double defaultValue, List<String> errors) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            double parsed = Double.parseDouble(value.trim());
            if (!Double.isFinite(parsed)) {
                throw new NumberFormatException(value);
            }
            return parsed;
        } catch (NumberFormatException e) {
            errors.add("invalid value '" + value + "' for " + name);
            return defaultValue;
        }
    }

    public static boolean parseBoolean(String value, String name, boolean defaultValue, List<String> errors) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        String trimmed = value.trim();
        if ("true".equalsIgnoreCase(trimmed)) {
            return true;
        }
        if ("false".equalsIgnoreCase(trimmed)) {
            return false;
        }
        errors.add("invalid value '" + value + "' for " + name);
        return defaultValue;
    }
}
