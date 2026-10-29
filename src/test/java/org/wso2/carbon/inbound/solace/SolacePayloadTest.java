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
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;

import org.apache.synapse.MessageContext;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The snapshot of a delivery: the content type resolution, the charset rule, one read of the message per
 * delivery and the user properties. No broker is needed, JCSMP messages are plain objects.
 */
public class SolacePayloadTest {

    private static final JCSMPFactory FACTORY = JCSMPFactory.onlyInstance();

    private static SynapseEnvironment environment;

    @BeforeClass
    public static void environment() throws Exception {
        environment = TestEnvironments.withPackBuilders();
    }

    private static SolaceInjectHandler handler(String contentType, boolean binaryPayloadAsBase64) {
        return new SolaceInjectHandler(null, null, environment, true, contentType, binaryPayloadAsBase64, "test");
    }

    private static SolaceUtils.Payload read(BytesXMLMessage message, String contentType) throws Exception {
        return SolaceUtils.readPayload(environment, message, contentType, false, Collections.emptyMap());
    }

    private static BytesMessage bytes(String declared, byte[] data) {
        BytesMessage message = FACTORY.createMessage(BytesMessage.class);
        if (declared != null) {
            message.setHTTPContentType(declared);
        }
        message.setData(data);
        return message;
    }

    private static String json(MessageContext context) {
        return JsonUtil.jsonPayloadToString(((Axis2MessageContext) context).getAxis2MessageContext());
    }

    @Test
    public void everyMessageClassHasItsDefaultContentType() throws Exception {
        TextMessage text = FACTORY.createMessage(TextMessage.class);
        text.setText("hello");
        assertEquals("text/plain", read(text, null).contentType);

        assertEquals("application/octet-stream", read(bytes(null, new byte[] {1}), null).contentType);

        XMLContentMessage xml = FACTORY.createMessage(XMLContentMessage.class);
        xml.setXMLContent("<a/>");
        assertEquals("application/xml", read(xml, null).contentType);

        MapMessage map = FACTORY.createMessage(MapMessage.class);
        SDTMap body = FACTORY.createMap();
        body.putString("k", "v");
        map.setMap(body);
        assertEquals("application/json", read(map, null).contentType);

        StreamMessage stream = FACTORY.createMessage(StreamMessage.class);
        SDTStream items = FACTORY.createStream();
        items.writeInteger(1);
        stream.setStream(items);
        assertEquals("application/json", read(stream, null).contentType);
    }

    @Test
    public void undeclaredContentIsNeverGuessed() throws Exception {
        TextMessage text = FACTORY.createMessage(TextMessage.class);
        text.setText("{\"a\":1}");
        SolaceUtils.Payload payload = read(text, null);
        assertEquals("text/plain", payload.contentType);
        assertTrue(payload.text);

        byte[] brace = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        SolaceUtils.Payload binary = read(bytes(null, brace), null);
        assertEquals("application/octet-stream", binary.contentType);
        assertFalse(binary.text);
        assertArrayEquals(brace, binary.bytes);

        byte[] angle = "<a/>".getBytes(StandardCharsets.UTF_8);
        assertEquals("application/octet-stream", read(bytes(null, angle), null).contentType);
    }

    @Test
    public void blankParameterCountsAsAbsent() throws Exception {
        byte[] data = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        assertEquals("application/json", read(bytes("application/json", data), null).contentType);
        assertEquals("application/json", read(bytes("application/json", data), "").contentType);
        assertEquals("application/json", read(bytes("application/json", data), "   ").contentType);
    }

    @Test
    public void parameterWinsOverTheDeclaration() throws Exception {
        byte[] data = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        assertEquals("text/plain", read(bytes("application/json", data), "text/plain").contentType);
        assertEquals("application/json", read(bytes(null, data), " application/json ").contentType);
    }

    @Test
    public void parameterWithoutCharsetInheritsTheDeclaredCharset() throws Exception {
        byte[] latin1 = "{\"n\":\"café\"}".getBytes(StandardCharsets.ISO_8859_1);
        SolaceUtils.Payload payload = read(bytes("application/json; charset=ISO-8859-1", latin1),
                "application/json");
        assertEquals("application/json; charset=ISO-8859-1", payload.contentType);
        assertArrayEquals("{\"n\":\"café\"}".getBytes(StandardCharsets.UTF_8), payload.bytes);
    }

    @Test
    public void parameterCharsetWinsOverTheDeclaredCharset() throws Exception {
        byte[] utf8 = "café".getBytes(StandardCharsets.UTF_8);
        SolaceUtils.Payload payload = read(bytes("text/plain; charset=ISO-8859-1", utf8),
                "text/plain; charset=UTF-8");
        assertEquals("text/plain; charset=UTF-8", payload.contentType);
        assertArrayEquals(utf8, payload.bytes);
    }

    @Test
    public void onlyTheCharsetIsInherited() throws Exception {
        SolaceUtils.Payload payload = read(
                bytes("text/plain; format=flowed; boundary=x1; charset=ISO-8859-1", new byte[] {'a'}),
                "text/csv");
        assertEquals("text/csv; charset=ISO-8859-1", payload.contentType);
    }

    @Test
    public void noCharsetAnywhereDecodesAsUtf8() throws Exception {
        byte[] utf8 = "café".getBytes(StandardCharsets.UTF_8);
        SolaceUtils.Payload payload = read(bytes("text/plain", utf8), null);
        assertArrayEquals(utf8, payload.bytes);
        assertTrue(payload.text);
    }

    @Test
    public void declaredCharsetIsDecodedAndHeldAsUtf8() throws Exception {
        SolaceUtils.Payload payload = read(bytes("text/plain; charset=ISO-8859-1", new byte[] {(byte) 0xE9}), null);
        assertArrayEquals(new byte[] {(byte) 0xC3, (byte) 0xA9}, payload.bytes);
        assertEquals("text/plain; charset=UTF-8", SolaceUtils.canonicalTextType(payload.contentType));
    }

    @Test
    public void producersOwnReplacementCharacterIsKept() throws Exception {
        byte[] data = "ok �".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(data, read(bytes("text/plain", data), null).bytes);
    }

    @Test
    public void malformedTextIsReportedNotReplaced() throws Exception {
        try {
            read(bytes("text/plain; charset=UTF-8", new byte[] {'o', 'k', (byte) 0xC3, (byte) 0x28}), null);
            fail("malformed input must be reported");
        } catch (CharacterCodingException expected) {
            // The build fails and the settlement table decides. No replacement character reaches the sequence.
        }
    }

    @Test
    public void unknownCharsetOfTextFailsTheBuild() throws Exception {
        try {
            read(bytes("text/plain; charset=no-such-charset", "ok".getBytes(StandardCharsets.UTF_8)), null);
            fail("an unknown charset must be reported");
        } catch (UnsupportedCharsetException expected) {
            // Same disposition as malformed input.
        }
    }

    @Test
    public void binaryBodyIsNeverDecoded() throws Exception {
        byte[] data = {(byte) 0xC3, (byte) 0x28, 0, (byte) 0xFF};
        SolaceUtils.Payload payload = read(bytes("application/pdf; charset=no-such-charset", data), null);
        assertFalse(payload.text);
        assertArrayEquals(data, payload.bytes);
    }

    @Test
    public void lonelySurrogateInATextMessageFailsTheBuild() throws Exception {
        TextMessage message = FACTORY.createMessage(TextMessage.class);
        message.setText("a\ud800b");
        try {
            read(message, null);
            fail("text that UTF-8 cannot carry must be reported");
        } catch (CharacterCodingException expected) {
            // Never written as a question mark.
        }
    }

    @Test
    public void legacyBase64OptionIsUnchangedByTheCharset() throws Exception {
        BytesMessage message = bytes("text/plain; charset=no-such-charset", new byte[] {(byte) 0xC3, (byte) 0x28});
        SolaceUtils.Payload payload = SolaceUtils.readPayload(environment, message, "application/json", true,
                Collections.emptyMap());
        assertTrue(payload.base64);
        assertEquals("application/octet-stream", payload.contentType);
        assertArrayEquals("wyg=".getBytes(StandardCharsets.US_ASCII), payload.bytes);
    }

    @Test
    public void legacyBase64OptionLeavesTextMessagesAlone() throws Exception {
        TextMessage message = FACTORY.createMessage(TextMessage.class);
        message.setText("hello");
        SolaceUtils.Payload payload = SolaceUtils.readPayload(environment, message, null, true, Collections.emptyMap());
        assertFalse(payload.base64);
        assertEquals("text/plain", payload.contentType);
    }

    @Test
    public void streamMessageIsReadOnceAndEveryAttemptGetsTheSameContent() throws Exception {
        StreamMessage message = FACTORY.createMessage(StreamMessage.class);
        SDTStream stream = FACTORY.createStream();
        stream.writeString("order42");
        stream.writeInteger(7);
        message.setStream(stream);
        SolaceInjectHandler handler = handler(null, false);

        SolaceUtils.Payload payload = handler.readPayload(message);
        assertArrayEquals("[\"order42\",7]".getBytes(StandardCharsets.UTF_8), payload.bytes);
        // The first conversion consumed the read cursor. A second read of the message body is empty.
        assertArrayEquals("[]".getBytes(StandardCharsets.UTF_8), handler.readPayload(message).bytes);

        MessageContext first = handler.buildContext(payload, message, 1);
        MessageContext second = handler.buildContext(payload, message, 2);
        assertEquals("[\"order42\",7]", json(first));
        assertEquals(json(first), json(second));
        assertEquals("1", first.getProperty(SolaceInboundConstants.SOLACE_DELIVERY_ATTEMPT));
        assertEquals("2", second.getProperty(SolaceInboundConstants.SOLACE_DELIVERY_ATTEMPT));
    }

    @Test
    public void nestedContainersAreReadOnce() throws Exception {
        MapMessage message = FACTORY.createMessage(MapMessage.class);
        SDTStream items = FACTORY.createStream();
        items.writeInteger(1);
        items.writeString("a");
        SDTMap body = FACTORY.createMap();
        body.putStream("items", items);
        message.setMap(body);
        SolaceInjectHandler handler = handler(null, false);

        SolaceUtils.Payload payload = handler.readPayload(message);
        assertArrayEquals("{\"items\":[1,\"a\"]}".getBytes(StandardCharsets.UTF_8), payload.bytes);
        // A second conversion of the same message finds the nested stream already read.
        assertArrayEquals("{\"items\":[]}".getBytes(StandardCharsets.UTF_8), handler.readPayload(message).bytes);

        assertEquals("{\"items\":[1,\"a\"]}", json(handler.buildContext(payload, message, 1)));
        assertEquals("{\"items\":[1,\"a\"]}", json(handler.buildContext(payload, message, 2)));
    }

    @Test
    public void everyAttemptBuildsFromTheSnapshotAndNeverReadsTheMessageAgain() throws Exception {
        BytesMessage message = bytes("text/plain", "first".getBytes(StandardCharsets.UTF_8));
        SDTMap properties = FACTORY.createMap();
        properties.putString("region", "EU");
        message.setProperties(properties);
        SolaceInjectHandler handler = handler(null, false);
        SolaceUtils.Payload payload = handler.readPayload(message);

        message.setData("changed".getBytes(StandardCharsets.UTF_8));
        SDTMap other = FACTORY.createMap();
        other.putString("region", "US");
        message.setProperties(other);

        for (int attempt = 1; attempt <= 2; attempt++) {
            MessageContext context = handler.buildContext(payload, message, attempt);
            assertEquals("first", context.getEnvelope().getBody().getFirstElement().getText());
            assertEquals("EU", context.getProperty(SolaceInboundConstants.SOLACE_USER_PROP_PREFIX + "region"));
        }
    }

    @Test
    public void byteUserPropertyBecomesBase64AndOtherTypesKeepTheirStringForm() throws Exception {
        TextMessage message = FACTORY.createMessage(TextMessage.class);
        message.setText("hello");
        SDTMap properties = FACTORY.createMap();
        properties.putBytes("bin", new byte[] {1, 2, 3});
        properties.putString("txt", "plain");
        properties.putInteger("num", 7);
        properties.putBoolean("flag", true);
        message.setProperties(properties);
        SolaceInjectHandler handler = handler(null, false);

        MessageContext context = handler.buildContext(handler.readPayload(message), message, 1);
        assertEquals("AQID", context.getProperty(SolaceInboundConstants.SOLACE_USER_PROP_PREFIX + "bin"));
        assertEquals("plain", context.getProperty(SolaceInboundConstants.SOLACE_USER_PROP_PREFIX + "txt"));
        assertEquals("7", context.getProperty(SolaceInboundConstants.SOLACE_USER_PROP_PREFIX + "num"));
        assertEquals("true", context.getProperty(SolaceInboundConstants.SOLACE_USER_PROP_PREFIX + "flag"));
    }

    @Test
    public void nestedUserPropertyIsReadOnceForEveryAttempt() throws Exception {
        TextMessage message = FACTORY.createMessage(TextMessage.class);
        message.setText("hello");
        SDTStream list = FACTORY.createStream();
        list.writeInteger(1);
        list.writeInteger(2);
        SDTMap properties = FACTORY.createMap();
        properties.putStream("list", list);
        message.setProperties(properties);
        SolaceInjectHandler handler = handler(null, false);

        SolaceUtils.Payload payload = handler.readPayload(message);
        String key = SolaceInboundConstants.SOLACE_USER_PROP_PREFIX + "list";
        Object first = handler.buildContext(payload, message, 1).getProperty(key);
        Object second = handler.buildContext(payload, message, 2).getProperty(key);
        assertEquals(payload.userProperties.get("list"), first);
        assertEquals(first, second);
    }

    @Test
    public void emptyPayloadBuildsNoContext() throws Exception {
        TextMessage text = FACTORY.createMessage(TextMessage.class);
        text.setText("");
        SolaceInjectHandler handler = handler(null, false);
        assertNull(handler.buildContext(handler.readPayload(text), text, 1));

        BytesMessage noData = FACTORY.createMessage(BytesMessage.class);
        assertNull(handler.buildContext(handler.readPayload(noData), noData, 1));

        BytesMessage emptyData = bytes("application/json", new byte[0]);
        assertNull(handler.buildContext(handler.readPayload(emptyData), emptyData, 1));

        SolaceInjectHandler legacy = handler(null, true);
        assertNull(legacy.buildContext(legacy.readPayload(emptyData), emptyData, 1));
    }

    @Test
    public void escapedQuoteInAParameterDoesNotHideTheCharset() throws Exception {
        String declared = "text/plain; profile=\"a\\\";b\"; charset=ISO-8859-1";
        assertEquals("ISO-8859-1", SolaceUtils.charsetParameter(declared));
        SolaceUtils.Payload payload = read(bytes(declared, "café".getBytes(StandardCharsets.ISO_8859_1)), null);
        assertArrayEquals("café".getBytes(StandardCharsets.UTF_8), payload.bytes);
        assertEquals("text/plain; profile=\"a\\\";b\"; charset=UTF-8",
                SolaceUtils.canonicalTextType(payload.contentType));
    }

    @Test
    public void semicolonInsideQuotesIsNotASeparator() throws Exception {
        String declared = "text/plain; title=\"x;charset=UTF-16\"; charset=ISO-8859-1";
        assertEquals("ISO-8859-1", SolaceUtils.charsetParameter(declared));
        assertArrayEquals("café".getBytes(StandardCharsets.UTF_8),
                read(bytes(declared, "café".getBytes(StandardCharsets.ISO_8859_1)), null).bytes);

        String quotedOnly = "text/plain; title=\"x;charset=UTF-16\"";
        assertNull(SolaceUtils.charsetParameter(quotedOnly));
        assertEquals("text/plain; title=\"x;charset=UTF-16\"; charset=UTF-8",
                SolaceUtils.canonicalTextType(quotedOnly));
    }

    @Test
    public void escapedBackslashEndsTheQuotedValue() throws Exception {
        String declared = "text/plain; path=\"a\\\\\"; charset=ISO-8859-1";
        assertEquals("ISO-8859-1", SolaceUtils.charsetParameter(declared));
        assertEquals("text/plain; path=\"a\\\\\"; charset=UTF-8", SolaceUtils.canonicalTextType(declared));
    }

    @Test
    public void charsetNameIsCaseInsensitiveAndAQuotedValueIsUnquoted() throws Exception {
        byte[] latin1 = "café".getBytes(StandardCharsets.ISO_8859_1);
        byte[] utf8 = "café".getBytes(StandardCharsets.UTF_8);
        assertEquals("iso-8859-1", SolaceUtils.charsetParameter("text/plain; ChArSeT=iso-8859-1"));
        assertArrayEquals(utf8, read(bytes("text/plain; ChArSeT=iso-8859-1", latin1), null).bytes);
        assertEquals("ISO-8859-1", SolaceUtils.charsetParameter("text/plain; charset=\"ISO-8859-1\""));
        assertArrayEquals(utf8, read(bytes("text/plain; charset=\"ISO-8859-1\"", latin1), null).bytes);
        assertEquals("ISO-8859-1", SolaceUtils.charsetParameter("text/plain; charset=\"ISO\\-8859-1\""));
        assertEquals("text/plain; format=flowed; charset=UTF-8",
                SolaceUtils.canonicalTextType("text/plain; CHARSET=\"ISO-8859-1\"; format=flowed"));
    }

    @Test
    public void overrideUsesTheSameParameterParser() throws Exception {
        byte[] latin1 = "café".getBytes(StandardCharsets.ISO_8859_1);
        String profile = "text/plain; profile=\"a\\\";b\"";
        SolaceUtils.Payload inherited = read(bytes("text/plain; charset=ISO-8859-1", latin1), profile);
        assertEquals(profile + "; charset=ISO-8859-1", inherited.contentType);
        assertArrayEquals("café".getBytes(StandardCharsets.UTF_8), inherited.bytes);

        String quotedSemicolon = "text/plain; title=\"x;charset=UTF-16\"";
        assertEquals(quotedSemicolon + "; charset=ISO-8859-1",
                read(bytes("text/plain; charset=ISO-8859-1", latin1), quotedSemicolon).contentType);

        String ownCharset = "text/plain; charset=\"ISO-8859-1\"";
        SolaceUtils.Payload own = read(bytes("text/plain; profile=\"a\\\";b\"; charset=UTF-8", latin1), ownCharset);
        assertEquals(ownCharset, own.contentType);
        assertArrayEquals("café".getBytes(StandardCharsets.UTF_8), own.bytes);
    }
}
