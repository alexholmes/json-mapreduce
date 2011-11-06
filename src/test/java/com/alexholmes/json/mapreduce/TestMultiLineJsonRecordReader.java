package com.alexholmes.json.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class TestMultiLineJsonRecordReader {

    private static Path outputDir;

    @BeforeClass
    public static void setUp() throws Exception {
        Path testBuildData = new Path(System.getProperty("test.build.data", "data"));
        outputDir = new Path(testBuildData, "outputDir");
    }

    @Test
    public void simple() throws IOException, InterruptedException {
        String json = "{\"c\": \"a\",\"v\": \"vv\"}";
        runTest(json, "c", Arrays.asList(json), Arrays.asList(1), 1024);
    }

    @Test
    public void simpleSplit() throws IOException, InterruptedException {
        // {"c": "a","v": "vv"}
        String json = "{\"c\": \"a\",\"v\": \"vv\"}";
        runTest(json, "c", Arrays.asList(json), Arrays.asList(1, 0, 0, 0), 5);
    }

    @Test
    public void splitDownTheMiddle() throws IOException, InterruptedException {
        // {"c": "a"}{"c": "b"}
        String jsonObj1 = "{\"c\": \"a\"}";
        String jsonObj2 = "{\"c\": \"b\"}";
        String json = jsonObj1 + jsonObj2;
        runTest(json, "c", Arrays.asList(jsonObj1, jsonObj2), Arrays.asList(1, 1), 10);
    }

    @Test
    public void simpleSplitSmallBlock() throws IOException, InterruptedException {
        // {"c": "a","v": "vv"}  (20)
        String jsonLeading = "{}{}{}";  // (6)
        String jsonTraling = "{\"c\": \"a\",\"v\": \"vv\"}";
        String json = jsonLeading + jsonTraling;
        runTest(json, "c", Arrays.asList(jsonTraling), Arrays.asList(0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0), 2);
        runTest(json, "c", Arrays.asList(jsonTraling), Arrays.asList(0, 0, 1, 0, 0, 0, 0, 0, 0, 0), 3);
        runTest(json, "c", Arrays.asList(jsonTraling), Arrays.asList(0, 1, 0, 0, 0, 0, 0), 4);
        runTest(json, "c", Arrays.asList(jsonTraling), Arrays.asList(0, 1, 0, 0, 0, 0), 5);
        runTest(json, "c", Arrays.asList(jsonTraling), Arrays.asList(0, 1, 0, 0, 0), 6);
        runTest(json, "c", Arrays.asList(jsonTraling), Arrays.asList(1, 0, 0, 0), 7);
        runTest(json, "c", Arrays.asList(jsonTraling), Arrays.asList(1, 0, 0, 0), 8);
        runTest(json, "c", Arrays.asList(jsonTraling), Arrays.asList(1, 0, 0), 12);
        runTest(json, "c", Arrays.asList(jsonTraling), Arrays.asList(1, 0), 24);
        runTest(json, "c", Arrays.asList(jsonTraling), Arrays.asList(1), 27);
    }

    @Test
    public void complexSplit() throws IOException, InterruptedException {
        String leadingObj = "[{\"f\":[{\"f1\":\"\",\"f2\":\"\"},{\"f1\":\"\",\"f2\":\"\"}]},";
        String largeComplexObj = "{\"color\":\"red\",\"v\":\"vv\",\"a\":true,\"b\":false,\"d\":null,\"e\":{\"e1\":\"\",\"\":\"\"},\"f\":[{\"f1\":\"\",\"f2\":\"\"},{\"f1\":\"\",\"f2\":\"\"}],\"name\":123.45,\"g\":[{\"f1\":\"\",\"f2\":{\"f11\":[{\"f12\":{}},{},{}]}},{\"f1\":\"\",\"f2\":\"\"}]}";
        String json = leadingObj + largeComplexObj + "]";

        for (int i = 2; i < json.length(); i++) {
            runTest(json, "name", Arrays.asList(largeComplexObj), null, i);
        }
    }


    private void runTest(String inputJson, String member, List<String> expectedOutputJson, List<Integer> expectedSplitRecords, long blockSize)
            throws IOException, InterruptedException {

        Configuration conf = new Configuration();
        conf.setLong("fs.local.block.size", blockSize);

        FileSystem localFs = FileSystem.getLocal(conf);
        localFs.delete(outputDir, true);
        localFs.mkdirs(outputDir);

        writeStringToFile(outputDir, localFs, inputJson);

        Job job = new Job(conf);


        MultiLineJsonInputFormat inputFormat = new MultiLineJsonInputFormat();
        MultiLineJsonInputFormat.setInputJsonMember(job, member);
        TextInputFormat.setInputPaths(job, outputDir);

        TaskAttemptContext attemptContext = new TaskAttemptContext(job.getConfiguration(),
                new TaskAttemptID("123", 0, true, 1, 2));

        List<InputSplit> is = inputFormat.getSplits(job);


        ListIterator<String> expectedResultsIter = expectedOutputJson.listIterator();
        ListIterator<Integer> expectedSplitRecordsIter = null;
        if (expectedSplitRecords != null) {
            expectedSplitRecordsIter = expectedSplitRecords.listIterator();
        }

        int splitNum = 0;
        for (InputSplit inputSplit : is) {
            RecordReader<LongWritable, Text> rr = inputFormat.createRecordReader(
                    inputSplit, attemptContext);
            rr.initialize(inputSplit, attemptContext);
            FileSplit split = (FileSplit) inputSplit;
            System.out.println("start = " + split.getStart() + " len = " + split.getLength());

            splitNum++;

            int count = 0;
            while (rr.nextKeyValue()) {
                count++;
                Text value = rr.getCurrentValue();

                if (!expectedResultsIter.hasNext()) {
                    assertTrue("Exected results doesn't have entry for '" + value + "', split " + splitNum +
                            ", split count " + count + ", block size " + blockSize, false);
                }

                String expectedJson = trimWhitespaces(expectedResultsIter.next());
                String actualJson = trimWhitespaces(value.toString());

                assertEquals(expectedJson, actualJson);

                System.out.println(value);
            }

            if (expectedSplitRecords != null) {
                //verify we have the right number of splits
                //
                if (!expectedSplitRecordsIter.hasNext()) {
                    assertTrue("Mismatch between number of splits " + is.size() +
                            " and expected splits" + expectedSplitRecords.size(), false);
                }

                assertEquals(expectedSplitRecordsIter.next().intValue(), count);
            }
            rr.close();
        }

        localFs.close();
    }

    private void writeStringToFile(Path outputDir, FileSystem fs, String s) throws IOException, InterruptedException {

        InputStream is = new ByteArrayInputStream(s.getBytes());
        OutputStream os = fs.create(new Path(outputDir, "input"));

        IOUtils.copyBytes(is, os, fs.getConf());

        IOUtils.closeStream(os);
        IOUtils.closeStream(is);
    }

    public String trimWhitespaces(String s) {
        return s.replaceAll("[\\n\\t\\r \\t]+", " ").trim();
    }
}
