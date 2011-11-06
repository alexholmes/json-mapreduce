package com.alexholmes.json.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for JSON text files.
 * Multi-line JSON is supported, and the JSON member name must be supplied.
 * <p/>
 * Keys are the position in the file, and values are the JSON object containing the member.
 */
public class MultiLineJsonInputFormat extends FileInputFormat<LongWritable, Text> {

    public static final String CONFIG_MEMBER_NAME = "multilinejsoninputformat.member";

    @Override
    public RecordReader<LongWritable, Text>
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) throws IOException {
        String member = context.getConfiguration().get(CONFIG_MEMBER_NAME);

        if (member == null) {
            throw new IOException("Missing configuration value for " + CONFIG_MEMBER_NAME);
        }
        return new MultiLineJsonRecordReader(member);
    }

    public static void setInputJsonMember(Job job, String member) {
        job.getConfiguration().set(CONFIG_MEMBER_NAME, member);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }

}
