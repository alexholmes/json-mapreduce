/*
 * Copyright 2013 Alex Holmes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alexholmes.json.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * An example MapReduce job showing how to use the {@link com.alexholmes.json.mapreduce.MultiLineJsonInputFormat}.
 */
public final class ExampleJob extends Configured implements Tool {

    /**
     * Main entry point for the example.
     *
     * @param args arguments
     * @throws Exception when something goes wrong
     */
    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ExampleJob(), args);
        System.exit(res);
    }

    /**
     * Sample input used by this example job.
     */
    public static final String JSON = "{\n" +
            "    \"colorsArray\":[{\n" +
            "            \"colorName\":\"red\",\n" +
            "            \"hexValue\":\"#f00\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"colorName\":\"green\",\n" +
            "            \"hexValue\":\"#0f0\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"colorName\":\"blue\",\n" +
            "            \"hexValue\":\"#00f\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"colorName\":\"cyan\",\n" +
            "            \"hexValue\":\"#0ff\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"colorName\":\"magenta\",\n" +
            "            \"hexValue\":\"#f0f\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"colorName\":\"yellow\",\n" +
            "            \"hexValue\":\"#ff0\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"colorName\":\"black\",\n" +
            "            \"hexValue\":\"#000\"\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    /**
     * Writes the contents of {@link #JSON} into a file in the job input directory in HDFS.
     *
     * @param conf     the Hadoop config
     * @param inputDir the HDFS input directory where we'll write a file
     * @throws IOException if something goes wrong
     */
    public static void writeInput(Configuration conf, Path inputDir) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(inputDir)) {
            throw new IOException(String.format("Input directory '%s' exists - please remove and rerun this example", inputDir));
        }

        OutputStreamWriter writer = new OutputStreamWriter(fs.create(new Path(inputDir, "input.txt")));
            writer.write(JSON);
        IOUtils.closeStream(writer);
    }

    /**
     * The MapReduce driver - setup and launch the job.
     *
     * @param args the command-line arguments
     * @return the process exit code
     * @throws Exception if something goes wrong
     */
    public int run(final String[] args) throws Exception {

        String input = args[0];
        String output = args[1];

        Configuration conf = super.getConf();

        writeInput(conf, new Path(input));

        Job job = new Job(conf);
        job.setJarByClass(ExampleJob.class);
        job.setMapperClass(Map.class);

        job.setNumReduceTasks(0);

        Path outputPath = new Path(output);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, outputPath);

        // use the JSON input format
        job.setInputFormatClass(MultiLineJsonInputFormat.class);

        // specify the JSON attribute name which is used to determine which
        // JSON elements are supplied to the mapper
        MultiLineJsonInputFormat.setInputJsonMember(job, "colorName");

        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    /**
     * JSON objects are supplied in string form to the mapper.
     * Here we are simply emitting them for viewing on HDFS.
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // strip-out newlines
            String formatted = value.toString().replaceAll("\n", " ");

            // emit the tuple and the original contents of the line
            context.write(new Text(String.format("Got JSON: '%s'", formatted)), null);
        }
    }
}
