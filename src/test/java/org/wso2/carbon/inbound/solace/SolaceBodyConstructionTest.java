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
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMException;
import org.apache.axiom.om.OMText;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.builder.ApplicationXMLBuilder;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.format.PlainTextBuilder;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.apache.synapse.transport.passthru.util.BinaryRelayBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.activation.DataHandler;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The body of the injected context is built by the builder registered for its content type, and the context
 * describes that body: messageType is the builder key, ContentType and the transport header carry the complete
 * type, setCharacterEncoding is false and no declaration encoding is left behind.
 */
public class SolaceBodyConstructionTest {

    private static final JCSMPFactory FACTORY = JCSMPFactory.onlyInstance();

    private static final String PAYLOAD_NS = "http://ws.apache.org/commons/ns/payload";

    private static final String BADGERFISH = "application/json/badgerfish";

    private static SynapseEnvironment environment;

    @BeforeClass
    public static void environment() throws Exception {
        environment = TestEnvironments.withPackBuilders();
    }

    private static SolaceUtils.Payload read(BytesXMLMessage message, String contentType) throws Exception {
        return read(environment, message, contentType);
    }

    private static SolaceUtils.Payload read(SynapseEnvironment in, BytesXMLMessage message, String contentType)
            throws Exception {
        return SolaceUtils.readPayload(in, message, contentType, false, Collections.emptyMap());
    }

    private static BytesMessage bytes(String declared, byte[] data) {
        BytesMessage message = FACTORY.createMessage(BytesMessage.class);
        if (declared != null) {
            message.setHTTPContentType(declared);
        }
        message.setData(data);
        return message;
    }

    private static MessageContext build(SolaceUtils.Payload payload) throws Exception {
        org.apache.synapse.MessageContext context = SolaceUtils.createMessageContext(environment, payload);
        return ((Axis2MessageContext) context).getAxis2MessageContext();
    }

    private static OMElement firstElement(MessageContext context) {
        return context.getEnvelope().getBody().getFirstElement();
    }

    private static byte[] binary(MessageContext context) throws Exception {
        OMElement wrapper = firstElement(context);
        assertEquals(PAYLOAD_NS, wrapper.getNamespace().getNamespaceURI());
        assertEquals("binary", wrapper.getLocalName());
        DataHandler handler = (DataHandler) ((OMText) wrapper.getFirstOMChild()).getDataHandler();
        try (InputStream in = handler.getInputStream()) {
            return in.readAllBytes();
        }
    }

    private static void assertLocations(MessageContext context, String messageType, String contentType) {
        assertEquals(messageType, context.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals(contentType, context.getProperty(Constants.Configuration.CONTENT_TYPE));
        assertEquals("false", context.getProperty(Constants.Configuration.SET_CONTENT_TYPE_CHARACTER_ENCODING));
        Map<?, ?> headers = (Map<?, ?>) context.getProperty(MessageContext.TRANSPORT_HEADERS);
        assertEquals(contentType, headers.get("Content-Type"));
        assertNull(context.getProperty(Constants.Configuration.XML_DECLARATION_ENCODING));
    }

    private static TextMessage text(String declared, String body) {
        TextMessage message = FACTORY.createMessage(TextMessage.class);
        message.setHTTPContentType(declared);
        message.setText(body);
        return message;
    }

    /**
     * Reads and builds on a separate thread and returns the failure, null when there is none. A builder that
     * never returns fails the test instead of hanging the suite. The thread is a daemon, so the test JVM
     * still exits.
     */
    private static Throwable boundedReadAndBuild(BytesXMLMessage message) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                build(read(message, null));
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "boundedBuild");
        worker.setDaemon(true);
        worker.start();
        worker.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse("the read and the build must return", worker.isAlive());
        return failure.get();
    }

    private static SynapseEnvironment withTestTextType() throws Exception {
        AxisConfiguration configuration = new AxisConfiguration();
        configuration.addMessageBuilder("application/x-test", new PlainTextBuilder());
        configuration.addMessageBuilder("application/octet-stream", new BinaryRelayBuilder());
        return new Axis2SynapseEnvironment(new ConfigurationContext(configuration), new SynapseConfiguration());
    }

    /**
     * The locations for a body whose formatter is a legacy JSON formatter. ContentType and the transport header
     * are the same complete type with one charset, setCharacterEncoding is false and the charset is also in the
     * character set encoding property.
     */
    private static void assertJsonFormatterLocations(MessageContext context, String messageType,
                                                     String contentType) {
        assertEquals(messageType, context.getProperty(Constants.Configuration.MESSAGE_TYPE));
        String described = (String) context.getProperty(Constants.Configuration.CONTENT_TYPE);
        assertEquals(contentType, described);
        assertEquals(described, 1, charsetCount(described));
        assertEquals("false", context.getProperty(Constants.Configuration.SET_CONTENT_TYPE_CHARACTER_ENCODING));
        assertEquals("UTF-8", context.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
        Map<?, ?> headers = (Map<?, ?>) context.getProperty(MessageContext.TRANSPORT_HEADERS);
        String header = (String) headers.get("Content-Type");
        assertEquals(described, header);
        assertEquals(header, 1, charsetCount(header));
    }

    private static int charsetCount(String contentType) {
        return contentType.toLowerCase(Locale.ROOT).split("charset=", -1).length - 1;
    }

    private static byte[] allBytes() {
        byte[] data = new byte[256];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        return data;
    }

    @Test
    public void mapMessageIsBuiltAsJson() throws Exception {
        MapMessage message = FACTORY.createMessage(MapMessage.class);
        SDTMap body = FACTORY.createMap();
        body.putString("id", "order42");
        message.setMap(body);

        MessageContext context = build(read(message, null));
        assertLocations(context, "application/json", "application/json; charset=UTF-8");
        assertEquals("UTF-8", context.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
        assertEquals("{\"id\":\"order42\"}", JsonUtil.jsonPayloadToString(context));
    }

    @Test
    public void declaredLatin1JsonIsBuiltFromCanonicalUtf8() throws Exception {
        byte[] latin1 = "{\"n\":\"café\"}".getBytes(StandardCharsets.ISO_8859_1);
        MessageContext context = build(read(bytes("application/json; charset=ISO-8859-1", latin1),
                "application/json"));
        assertLocations(context, "application/json", "application/json; charset=UTF-8");
        assertEquals("{\"n\":\"café\"}", JsonUtil.jsonPayloadToString(context));
    }

    @Test
    public void textMessageIsBuiltAsText() throws Exception {
        TextMessage message = FACTORY.createMessage(TextMessage.class);
        message.setText("hello café");

        MessageContext context = build(read(message, null));
        assertLocations(context, "text/plain", "text/plain; charset=UTF-8");
        assertEquals("text", firstElement(context).getLocalName());
        assertEquals("hello café", firstElement(context).getText());
    }

    @Test
    public void xmlContentMessageIsBuiltAsXml() throws Exception {
        XMLContentMessage message = FACTORY.createMessage(XMLContentMessage.class);
        message.setXMLContent("<order><id>42</id></order>");

        MessageContext context = build(read(message, null));
        assertLocations(context, "application/xml", "application/xml; charset=UTF-8");
        assertEquals("order", firstElement(context).getLocalName());
        assertEquals("42", firstElement(context).getFirstElement().getText());
    }

    @Test
    public void declarationEncodingIsRemovedAfterTheBuild() throws Exception {
        byte[] latin1 = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><n>café</n>"
                .getBytes(StandardCharsets.ISO_8859_1);
        SolaceUtils.Payload payload = read(bytes("application/xml; charset=ISO-8859-1", latin1), null);

        // Control: the XML builder records the encoding the document declares when it differs from the
        // charset of the bytes.
        MessageContext control = new MessageContext();
        control.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, "UTF-8");
        new ApplicationXMLBuilder().processDocument(new ByteArrayInputStream(payload.bytes),
                "application/xml; charset=UTF-8", control);
        assertEquals("ISO-8859-1", control.getProperty(Constants.Configuration.XML_DECLARATION_ENCODING));

        MessageContext context = build(payload);
        assertLocations(context, "application/xml", "application/xml; charset=UTF-8");
        assertEquals("café", firstElement(context).getText());
    }

    @Test
    public void undeclaredBytesAreTheNativeBinaryElementForEveryAttempt() throws Exception {
        byte[] data = allBytes();
        SolaceUtils.Payload payload = read(bytes(null, data), null);

        MessageContext first = build(payload);
        MessageContext second = build(payload);
        assertLocations(first, "application/octet-stream", "application/octet-stream");
        assertArrayEquals(data, binary(first));
        assertArrayEquals(data, binary(second));
    }

    @Test
    public void dataSourceBuilderGetsARepeatableSource() throws Exception {
        byte[] data = allBytes();
        MessageContext context = build(read(bytes("application/binary", data), null));
        assertLocations(context, "application/binary", "application/binary");
        assertArrayEquals(data, binary(context));
        assertArrayEquals(data, binary(context));
    }

    @Test
    public void unregisteredTypesTakeTheFamilyFallback() throws Exception {
        MessageContext csv = build(read(bytes("text/csv", "a,b".getBytes(StandardCharsets.UTF_8)), null));
        assertLocations(csv, "text/plain", "text/csv; charset=UTF-8");
        assertEquals("a,b", firstElement(csv).getText());

        MessageContext problem = build(read(bytes("application/problem+json",
                "{\"title\":\"x\"}".getBytes(StandardCharsets.UTF_8)), null));
        assertLocations(problem, "application/json", "application/problem+json; charset=UTF-8");
        assertEquals("{\"title\":\"x\"}", JsonUtil.jsonPayloadToString(problem));

        MessageContext vendorXml = build(read(bytes("application/vnd.test+xml",
                "<t/>".getBytes(StandardCharsets.UTF_8)), null));
        assertLocations(vendorXml, "application/xml", "application/vnd.test+xml; charset=UTF-8");
        assertEquals("t", firstElement(vendorXml).getLocalName());

        byte[] data = allBytes();
        MessageContext custom = build(read(bytes("application/x-custom-binary", data), null));
        assertLocations(custom, "application/octet-stream", "application/x-custom-binary");
        assertArrayEquals(data, binary(custom));
    }

    @Test
    public void registeredBuilderWinsOverTheFamilyFallback() throws Exception {
        AxisConfiguration configuration = new AxisConfiguration();
        configuration.addMessageBuilder("text/plain", new PlainTextBuilder());
        configuration.addMessageBuilder("application/x-custom", new PlainTextBuilder());
        SynapseEnvironment custom = new Axis2SynapseEnvironment(new ConfigurationContext(configuration),
                new SynapseConfiguration());

        org.apache.synapse.MessageContext context = SolaceUtils.createMessageContext(custom,
                read(custom, bytes("application/x-custom", "marker".getBytes(StandardCharsets.UTF_8)), null));
        MessageContext axis2 = ((Axis2MessageContext) context).getAxis2MessageContext();
        // The plain text builder reads characters, so the bytes are decoded and declared as canonical UTF-8.
        assertLocations(axis2, "application/x-custom", "application/x-custom; charset=UTF-8");
        assertEquals("marker", firstElement(axis2).getText());
    }

    @Test
    public void soapTypesAreBuiltAsPlainXml() throws Exception {
        String envelope = "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\">"
                + "<s:Body><a/><b/></s:Body></s:Envelope>";
        MessageContext context = build(read(bytes("text/xml", envelope.getBytes(StandardCharsets.UTF_8)), null));
        assertLocations(context, "application/xml", "text/xml; charset=UTF-8");
        assertEquals("Envelope", firstElement(context).getLocalName());

        MessageContext soap12 = build(read(bytes("application/soap+xml; action=\"urn:x\"",
                "<r/>".getBytes(StandardCharsets.UTF_8)), null));
        assertLocations(soap12, "application/xml", "application/soap+xml; action=\"urn:x\"; charset=UTF-8");
    }

    @Test
    public void formsTakeTheOpaqueBinaryPath() throws Exception {
        String declared = "application/x-www-form-urlencoded; charset=ISO-8859-1";
        byte[] data = "a+b=caf%E9".getBytes(StandardCharsets.US_ASCII);
        MessageContext form = build(read(bytes(declared, data), null));
        assertLocations(form, "application/octet-stream", declared);
        assertEquals("ISO-8859-1", form.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
        assertArrayEquals(data, binary(form));

        String multipart = "multipart/form-data; boundary=xyz";
        byte[] parts = "--xyz\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\n1\r\n--xyz--\r\n"
                .getBytes(StandardCharsets.US_ASCII);
        MessageContext multi = build(read(bytes(multipart, parts), null));
        assertLocations(multi, "application/octet-stream", multipart);
        assertArrayEquals(parts, binary(multi));
    }

    @Test
    public void aFormTextMessageKeepsItsDeclaredCharset() throws Exception {
        String latin1 = "application/x-www-form-urlencoded; charset=ISO-8859-1";
        MessageContext form = build(read(text(latin1, "name=caf%E9"), null));
        assertLocations(form, "application/octet-stream", latin1);
        assertEquals("ISO-8859-1", form.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
        assertArrayEquals("name=caf%E9".getBytes(StandardCharsets.ISO_8859_1), binary(form));

        // The same form as bytes gives the same body.
        MessageContext asBytes = build(read(bytes(latin1, "name=caf%E9".getBytes(StandardCharsets.US_ASCII)), null));
        assertArrayEquals(binary(form), binary(asBytes));

        // A charset inherited through the endpoint parameter is kept as well.
        MessageContext inherited = build(read(text(latin1, "name=caf%E9"), "application/x-www-form-urlencoded"));
        assertLocations(inherited, "application/octet-stream", latin1);

        // With no charset the text is encoded as UTF-8 and the declaration gains nothing.
        String plain = "application/x-www-form-urlencoded";
        MessageContext utf8 = build(read(text(plain, "name=caf%C3%A9&v=\u00e9"), null));
        assertLocations(utf8, "application/octet-stream", plain);
        assertArrayEquals("name=caf%C3%A9&v=\u00e9".getBytes(StandardCharsets.UTF_8), binary(utf8));
    }

    @Test
    public void aFormTextItsCharsetCannotEncodeFailsTheBuild() throws Exception {
        try {
            read(text("application/x-www-form-urlencoded; charset=ISO-8859-1", "name=\u540d"), null);
            fail("a form text its charset cannot encode must fail the read");
        } catch (CharacterCodingException expected) {
            // The existing build failure path takes it, nothing is replaced.
        }
    }

    @Test
    public void legacyBase64OptionKeepsTheTextBody() throws Exception {
        SolaceUtils.Payload payload = SolaceUtils.readPayload(environment,
                bytes("application/json", new byte[] {1, 2, 3}), null, true, Collections.emptyMap());
        MessageContext context = build(payload);
        assertEquals("application/octet-stream", context.getProperty(Constants.Configuration.MESSAGE_TYPE));
        assertEquals("application/octet-stream", context.getProperty(Constants.Configuration.CONTENT_TYPE));
        // The 2.0.0 element: message in the payload namespace, holding the base64 text.
        assertEquals("message", firstElement(context).getLocalName());
        assertEquals(PAYLOAD_NS, firstElement(context).getNamespace().getNamespaceURI());
        assertEquals("AQID", firstElement(context).getText());
    }

    @Test
    public void malformedXmlRootFailsTheBuild() throws Exception {
        SolaceUtils.Payload payload = read(bytes("application/xml", "not xml".getBytes(StandardCharsets.UTF_8)),
                null);
        try {
            build(payload);
            fail("a malformed root must fail the build");
        } catch (OMException expected) {
            // The worker logs the build failure and applies the injection rejected row.
        }
    }

    @Test
    public void xmlMalformedAfterItsRootIsBuiltAndFailsWhereItIsRead() throws Exception {
        MessageContext context = build(read(bytes("application/xml",
                "<root><item>value</root>".getBytes(StandardCharsets.UTF_8)), null));
        assertEquals("root", firstElement(context).getLocalName());
        try {
            firstElement(context).toString();
            fail("the rest of the document must fail where it is first read");
        } catch (OMException expected) {
            // The sequence meets the failure and the settlement table decides.
        }
    }

    @Test
    public void malformedJsonFailsTheBuild() throws Exception {
        SolaceUtils.Payload payload = read(bytes("application/json", "not json".getBytes(StandardCharsets.UTF_8)),
                null);
        try {
            build(payload);
            fail("JSON the builder rejects must fail the build");
        } catch (AxisFault expected) {
            // The worker logs the build failure and applies the injection rejected row.
        }
    }

    @Test
    public void jsonTheBuilderAcceptsAtItsStartIsBuiltAndFailsWhereItIsRead() throws Exception {
        MessageContext context = build(read(bytes("application/json",
                "{not valid JSON".getBytes(StandardCharsets.UTF_8)), null));
        assertEquals("jsonObject", firstElement(context).getLocalName());
        try {
            firstElement(context).getFirstElement();
            fail("the rest of the document must fail where it is first read");
        } catch (Exception expected) {
            // The platform JSON builder checks the first character only. The sequence meets the failure.
        }
    }

    @Test
    public void noRegisteredBuilderFailsTheBuild() throws Exception {
        try {
            SynapseEnvironment none = TestEnvironments.withoutBuilders();
            SolaceUtils.createMessageContext(none, read(none, bytes(null, new byte[] {1}), null));
            fail("a pack without a builder for the type must fail the build");
        } catch (AxisFault expected) {
            assertTrue(expected.getMessage().contains("application/octet-stream"));
        }
    }

    @Test
    public void everyAttemptHasItsOwnContextAndReader() throws Exception {
        SolaceUtils.Payload payload = read(bytes("text/plain", "same".getBytes(StandardCharsets.UTF_8)), null);
        MessageContext first = build(payload);
        MessageContext second = build(payload);
        assertFalse(first == second);
        assertFalse(first.getEnvelope() == second.getEnvelope());
        assertEquals("same", firstElement(second).getText());
        assertEquals("same", firstElement(first).getText());
    }

    @Test
    public void incompleteBadgerfishFailsTheBuildAndReturns() throws Exception {
        byte[] complete = "{\"n\":{\"$\":\"café\"}}".getBytes(StandardCharsets.UTF_8);
        // Every proper prefix is cut somewhere inside the value. A cut inside the two bytes of é fails the
        // strict decoding, every other cut fails the JSON check. None reaches the legacy builder.
        for (int length = 1; length < complete.length; length++) {
            byte[] cut = Arrays.copyOf(complete, length);
            Throwable failure = boundedReadAndBuild(bytes(BADGERFISH, cut));
            assertTrue("cut at " + length + " must fail the build", failure instanceof IOException);
        }
        String[] incomplete = {"{\"n\"", "{\"p:n\"", "{\"n\"}", " {\"n\"", "{\"n\":1} {", "{\"n\":1}{\"m\":2}"};
        for (String body : incomplete) {
            assertTrue(body, boundedReadAndBuild(bytes(BADGERFISH, body.getBytes(StandardCharsets.UTF_8)))
                    instanceof AxisFault);
            assertTrue(body, boundedReadAndBuild(text(BADGERFISH, body)) instanceof AxisFault);
        }
        // Repeated failures leave nothing behind. The next valid delivery is built by the registered builder.
        MessageContext next = build(read(bytes(BADGERFISH,
                "{\"n\":{\"$\":\"ok\"}}".getBytes(StandardCharsets.UTF_8)), null));
        assertEquals("n", firstElement(next).getLocalName());
        assertEquals("ok", firstElement(next).getText());
    }

    @Test
    public void validBadgerfishIsBuiltByItsRegisteredBuilderForEveryAttempt() throws Exception {
        SolaceUtils.Payload payload = read(bytes(BADGERFISH,
                "{\"n\":{\"$\":\"café\"}}".getBytes(StandardCharsets.UTF_8)), null);
        assertNull(boundedReadAndBuild(bytes(BADGERFISH,
                "{\"n\":{\"$\":\"café\"}}".getBytes(StandardCharsets.UTF_8))));
        MessageContext first = build(payload);
        MessageContext second = build(payload);
        assertJsonFormatterLocations(first, BADGERFISH, BADGERFISH + "; charset=UTF-8");
        assertEquals("n", firstElement(first).getLocalName());
        assertEquals("café", firstElement(first).getText());
        assertEquals("café", firstElement(second).getText());
    }

    @Test
    public void textMessageUnderBadgerfishIsBuiltFromCanonicalUtf8() throws Exception {
        MessageContext context = build(read(text(BADGERFISH + "; charset=ISO-8859-1",
                "{\"n\":{\"$\":\"café\"}}"), null));
        assertJsonFormatterLocations(context, BADGERFISH, BADGERFISH + "; charset=UTF-8");
        assertEquals("café", firstElement(context).getText());
    }

    @Test
    public void badgerfishBytesAreDecodedStrictly() throws Exception {
        byte[] latin1 = "{\"n\":{\"$\":\"café\"}}".getBytes(StandardCharsets.ISO_8859_1);
        MessageContext context = build(read(bytes(BADGERFISH + "; charset=ISO-8859-1", latin1), null));
        assertJsonFormatterLocations(context, BADGERFISH, BADGERFISH + "; charset=UTF-8");
        assertEquals("café", firstElement(context).getText());

        byte[] replacement = "{\"n\":{\"$\":\"ok \uFFFD\"}}".getBytes(StandardCharsets.UTF_8);
        assertEquals("ok \uFFFD", firstElement(build(read(bytes(BADGERFISH, replacement), null))).getText());

        byte[] malformed = {'{', '"', 'n', '"', ':', '"', (byte) 0xC3, (byte) 0x28, '"', '}'};
        try {
            read(bytes(BADGERFISH, malformed), null);
            fail("malformed UTF-8 must be reported");
        } catch (CharacterCodingException expected) {
            // The build fails. No replacement character reaches the sequence.
        }
    }

    @Test
    public void registeredCharacterBuilderGetsCanonicalUtf8() throws Exception {
        SynapseEnvironment custom = withTestTextType();
        String declared = "application/x-test; charset=ISO-8859-1";

        MessageContext fromText = ((Axis2MessageContext) SolaceUtils.createMessageContext(custom,
                read(custom, text(declared, "café"), null))).getAxis2MessageContext();
        assertLocations(fromText, "application/x-test", "application/x-test; charset=UTF-8");
        assertEquals("UTF-8", fromText.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
        assertEquals("café", firstElement(fromText).getText());

        MessageContext fromBytes = ((Axis2MessageContext) SolaceUtils.createMessageContext(custom,
                read(custom, bytes(declared, "café".getBytes(StandardCharsets.ISO_8859_1)), null)))
                .getAxis2MessageContext();
        assertLocations(fromBytes, "application/x-test", "application/x-test; charset=UTF-8");
        assertEquals("UTF-8", fromBytes.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
        assertEquals("café", firstElement(fromBytes).getText());

        try {
            read(custom, bytes("application/x-test", new byte[] {'o', 'k', (byte) 0xC3, (byte) 0x28}), null);
            fail("malformed UTF-8 must be reported");
        } catch (CharacterCodingException expected) {
            // Same disposition as malformed input of the text family.
        }
    }

    @Test
    public void textMessageUnderABinaryTypeIsDeclaredAsUtf8() throws Exception {
        MessageContext context = build(read(text("application/octet-stream; charset=ISO-8859-1", "café"), null));
        assertLocations(context, "application/octet-stream", "application/octet-stream; charset=UTF-8");
        assertEquals("UTF-8", context.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
        assertArrayEquals("café".getBytes(StandardCharsets.UTF_8), binary(context));
    }

    @Test
    public void bytesOfABinaryBuilderKeepTheirDeclaration() throws Exception {
        byte[] data = allBytes();
        MessageContext context = build(read(bytes("application/binary; charset=ISO-8859-1", data), null));
        assertLocations(context, "application/binary", "application/binary; charset=ISO-8859-1");
        assertEquals("ISO-8859-1", context.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
        assertArrayEquals(data, binary(context));
    }

    @Test
    public void legacyJsonTypesCarryOneCharsetInContentType() throws Exception {
        String[][] cases = {
                {"text/javascript", "text/javascript; charset=UTF-8"},
                {"text/javascript; charset=ISO-8859-1", "text/javascript; charset=UTF-8"},
                {"text/javascript; profile=x; charset=ISO-8859-1", "text/javascript; profile=x; charset=UTF-8"},
                {BADGERFISH, BADGERFISH + "; charset=UTF-8"},
                {BADGERFISH + "; charset=ISO-8859-1", BADGERFISH + "; charset=UTF-8"},
                {BADGERFISH + "; profile=x", BADGERFISH + "; profile=x; charset=UTF-8"}};
        for (String[] row : cases) {
            String declared = row[0];
            String body = declared.startsWith(BADGERFISH) ? "{\"n\":{\"$\":\"café\"}}" : "{\"n\":\"café\"}";
            MessageContext context = build(read(bytes(declared, body.getBytes(SolaceUtils.charsetOf(declared))), null));
            String messageType = SolaceUtils.baseType(declared);
            assertJsonFormatterLocations(context, messageType, row[1]);
        }
    }
}
