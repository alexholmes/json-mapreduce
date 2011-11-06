package com.alexholmes.json.parser;

import org.apache.commons.lang.mutable.MutableInt;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static junit.framework.Assert.*;


public class PartitionedJsonParserMaxObjectOld {
              /*
    @Test
    public void testSingleLargeObject() throws IOException {
        InputStream jsonInputStream = createFromString(PartitionedJsonParserMaxStreamOld.json);
        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream, 5, Long.MAX_VALUE);
        assertNull(parser.nextObjectContainingMember("name"));
        assertNotSame(5, parser.getBytesRead());
    }


    public static String json_long = "{\"c\": \"a\",\"aasdfdfadgsdfgsdgdgdshdghsfdghsfghdfgsdfgdgf\": \"bsdfgsdsfgsdgdsgdsgdsfgdfsgdfgsdgdsfgsdgdsgdsgf\",\"v\": \"vv\"}";

    public static String json_short1 = "{\"c\": \"a\",\"v\": \"vv\"}";
    public static String json_short2 = "{\"x\": \"a\"}";
    public static String json_short3 = "{\"c\": \"a\"}";

    public static String json2 = "[" + json_short1 + "," + json_long + "," + json_short2 + "," + json_short3 + "]";

    @Test
    public void testEnsureGood() throws IOException {
        InputStream jsonInputStream = createFromString(json2);
        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream);
        assertEquals(json_short1, parser.nextObjectContainingMember("c"));
        assertEquals(json_long, parser.nextObjectContainingMember("c"));
        assertEquals(json_short3, parser.nextObjectContainingMember("c"));
    }

    @Test
    public void testPartial() throws IOException {
        InputStream jsonInputStream = createFromString(json2);
        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream, 30, Long.MAX_VALUE);
        assertEquals(json_short1, parser.nextObjectContainingMember("c"));
        assertEquals(json_short3, parser.nextObjectContainingMember("c"));
    }

    @Test
    public void testPartialCallback() throws IOException {
        InputStream jsonInputStream = createFromString(json2);
        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream, 30, Long.MAX_VALUE);

        final MutableInt objectSizeClbk = new MutableInt(0);
        final StringBuilder objectFragmentClbk = new StringBuilder();
        final MutableInt objectStartByteOffsetClbk = new MutableInt(0);

        parser.setObjectTooLongCallback(new JsonObjectTooLongCallback() {
            public void objectTooLong(long objectSize, String objectFragment, long objectStartByteOffset) {
                objectSizeClbk.setValue(objectSize);
                objectFragmentClbk.append(objectFragment);
                objectStartByteOffsetClbk.setValue(objectStartByteOffset);
            }
        });
        assertEquals(json_short1, parser.nextObjectContainingMember("c"));
        assertEquals(json_short3, parser.nextObjectContainingMember("c"));

        String truncated = json_long.substring(0, 30);
        assertEquals(truncated, objectFragmentClbk.toString());
        assertTrue(objectSizeClbk.intValue() > 30);
        assertNotSame(0, objectStartByteOffsetClbk.intValue());
    }


    public InputStream createFromString(String s) {
        return new ByteArrayInputStream(s.getBytes());
    }                    */
}
