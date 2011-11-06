package com.alexholmes.json.parser;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static junit.framework.Assert.*;


public class PartitionedJsonParserMaxStreamOld {
                 /*
    public static String json = "[\n" +
                "    {\n" +
                "        \"color\": \"red\",\n" +
                "        \"v\": \"vv\",\n" +
                "        \"a\": true,\n" +
                "        \"name\": false,\n" +
                "        \"c\": 123.45,\n" +
                "        \"d\": null,\n" +
                "        \"e\": {\n" +
                "            \"e1\":\"\",\n" +
                "            \"\":\"\"\n" +
                "        },\n" +
                "        \"f\": [\n" +
                "            {\n" +
                "                \"f1\":\"\",\n" +
                "                \"f2\":\"\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"f1\":\"\",\n" +
                "                \"f2\":\"\"\n" +
                "            }\n" +
                "        ]\n" +
                "    }\n" +
                "]";
    public static String json2 = "[{\"color\": \"red\",\"v\": \"vv\"},{\"color\": \"red\",\"v\": \"vv\"}]";

    @Test
    public void testEnsureGood() throws IOException {
        InputStream jsonInputStream = createFromString(json);
        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream);
        assertNotNull(parser.nextObjectContainingMember("name"));
        assertNull(parser.nextObjectContainingMember("name"));

        jsonInputStream = createFromString(json2);
        parser = new PartitionedJsonParser(jsonInputStream);
        assertNotNull(parser.nextObjectContainingMember("color"));
        assertNotNull(parser.nextObjectContainingMember("color"));
        assertNull(parser.nextObjectContainingMember("color"));
    }

    @Test
    public void testSingleLargeObject() throws IOException {
        InputStream jsonInputStream = createFromString(json);
        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream, Integer.MAX_VALUE, 5);
        assertNull(parser.nextObjectContainingMember("name"));
        assertEquals(5, parser.getBytesRead());
    }

    @Test
    public void testPartial() throws IOException {
        InputStream jsonInputStream = createFromString(json2);
        PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream, Integer.MAX_VALUE, 50);
        assertNotNull(parser.nextObjectContainingMember("color"));
        assertNull(parser.nextObjectContainingMember("color"));
        assertEquals(50, parser.getBytesRead());
    }

    // test the case where the start-object is before the max stream size, but the member was after the


    public InputStream createFromString(String s) {
        return new ByteArrayInputStream(s.getBytes());
    }    */
}
