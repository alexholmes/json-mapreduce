package com.alexholmes.json.parser;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.*;

import static junit.framework.Assert.*;


public class PartitionedJsonParserSeekTest {

    @Test
    public void testNoSeek() throws IOException {
        File testsDir = new File("src/test/resources/parser-tests/seek");
        File[] dirs= testsDir.listFiles();

        for(File jsonDir: dirs) {
            runTest(jsonDir);
        }
    }

    public void runTest(final File jsonDir) throws IOException {

        File jsonFile = new File(jsonDir, "input.json");
        InputStream jsonInputStream = new FileInputStream(jsonFile);

        try {
            seekToStart(jsonInputStream);
            PartitionedJsonParser parser = new PartitionedJsonParser(jsonInputStream);

            File[] jsonOjbectFiles = jsonFile.getParentFile().listFiles(new FilenameFilter() {
                public boolean accept(File file, String s) {
                    return s.contains("expected");
                }
            });

            if(jsonOjbectFiles == null || jsonOjbectFiles.length == 0) {
                String result = parser.nextObjectContainingMember("name");
                assertNull("File " + jsonFile.getAbsolutePath() + " got result '" + result + "'", result);
                System.out.println("File " + jsonFile.getAbsolutePath() + " passed");
            } else {
                for(File jsonObjectFile: jsonOjbectFiles) {
                    String expected = trimWhitespaces(FileUtils.readFileToString(jsonObjectFile));
                    String result = parser.nextObjectContainingMember("name");
                    assertNotNull(jsonFile.getAbsolutePath() + "/" + jsonObjectFile.getName(), result);
                    assertEquals(jsonFile.getAbsolutePath() + "/" + jsonObjectFile.getName(), expected, trimWhitespaces(result));
                    System.out.println("File " + jsonFile.getAbsolutePath() + "/" + jsonObjectFile.getName() + " passed");
                }
            }

        } finally {
            IOUtils.closeQuietly(jsonInputStream);
        }
    }

    public void seekToStart(InputStream jsonInputStream) throws IOException {
        // pop off characters until we see <SEEK>
        //
        StringBuilder sb = new StringBuilder();
        int i;
        while((i = jsonInputStream.read()) != -1) {
            sb.append((char)i);

            if(sb.toString().endsWith("<SEEK>")) {
                return;
            }
        }
        assertTrue(false);
    }

    public String trimWhitespaces(String s) {
        return s.replaceAll("[\\n\\t\\r \\t]+", " ").trim();
    }

}
