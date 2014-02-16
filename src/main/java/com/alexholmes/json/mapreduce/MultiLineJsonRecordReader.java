package com.alexholmes.json.mapreduce;

import com.alexholmes.json.parser.PartitionedJsonParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

/**
 * Extracts JSON objects from multiple splits, where keys are offset in file and value is the JSON object as a string.
 */
public class MultiLineJsonRecordReader extends RecordReader<LongWritable, Text> {
    private static final Log log = LogFactory.getLog(MultiLineJsonRecordReader.class);

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private int maxObjectLength;
    private LongWritable key;
    private Text value;
    private InputStream is;
    private PartitionedJsonParser parser;
    private final String jsonMemberName;

    public MultiLineJsonRecordReader(String jsonMemberName) {
        this.jsonMemberName = jsonMemberName;
    }

    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = HadoopCompat.getConfiguration(context);
        this.maxObjectLength = job.getInt("mapred.multilinejsonrecordreader.maxlength", Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        if (codec != null) {
            is = codec.createInputStream(fileIn);
            start = 0;
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                fileIn.seek(start);
            }
            is = fileIn;
        }
        parser = new PartitionedJsonParser(is);
        this.pos = start;
    }

    public boolean nextKeyValue() throws IOException {
        if (pos >= end) {
            key = null;
            value = null;
            return false;
        }

        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new Text();
        }

        while(pos < end) {

            String json = parser.nextObjectContainingMember(jsonMemberName);
            pos = start + parser.getBytesRead();

            if (json == null) {
                key = null;
                value = null;
                return false;
            }

            long jsonStart = pos - json.length();

            // if the "begin-object" position is after the end of our split,
            // we should ignore it
            //
            if(jsonStart >= end) {
                key = null;
                value = null;
                return false;
            }

            if(json.length() > maxObjectLength) {
                log.info("Skipped JSON object of size " + json.length() + " at pos " + jsonStart);
            } else {
                key.set(jsonStart);
                value.set(json);
                return true;
            }
        }

        key = null;
        value = null;
        return false;
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    /**
     * Get the progress within the split
     */
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public synchronized void close() throws IOException {
        if (is != null) {
            is.close();
        }
    }
}

