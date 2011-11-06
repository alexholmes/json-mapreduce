An InputFormat to work with splittable multi-line JSON
======================================================

## Motivation

Currently there don't seem to be any JSON InputFormat classes that can support multi-line JSON.

## License

Apache licensed.

## Usage

To get started, simply:

1. Download, and run ant
2. Include the `dist/lib/json-mapreduce-1.0.jar` in your environment
3. Utilize the `MultiLineJsonInputFormat` class as your Mapper InputFormat


Assume you have some JSON that looks like this:

<pre><code>{"menu": {
    "header": "SVG Viewer",
    "items": [
        {"id": "Open"},
        {"id": "OpenNew", "label": "Open New"},
        null,
        {"id": "ZoomIn", "label": "Zoom In"},
        {"id": "ZoomOut", "label": "Zoom Out"},
        {"id": "OriginalView", "label": "Original View"},
        null,
        {"id": "Quality"},
        {"id": "Pause"},
        {"id": "Mute"},
        null,
        {"id": "Find", "label": "Find..."},
        {"id": "FindAgain", "label": "Find Again"},
        {"id": "Copy"},
        {"id": "CopyAgain", "label": "Copy Again"},
        {"id": "CopySVG", "label": "Copy SVG"},
        {"id": "ViewSVG", "label": "View SVG"},
        {"id": "ViewSource", "label": "View Source"},
        {"id": "SaveAs", "label": "Save As"},
        null,
        {"id": "Help"},
        {"id": "About", "label": "About Adobe CVG Viewer..."}
    ]
}}</code></pre>

With the MultiLineJsonInputFormat you must indicate the member name which it will use to determine the
encapsulating object to return to your Mapper.   If for example we wanted all the objects that contained
`"id"`, then we would do the following:

<pre><code>Configuration conf = new Configuration();
Job job = new Job(conf);
job.setMapperClass(...);
job.setReducerClass(...);
job.setInputFormatClass(MultiLineJsonInputFormat.class);
MultiLineJsonInputFormat.setInputJsonMember(job, "id");
</code></pre>

The MultiLineJsonInputFormat supplies the Mapper with the JSON object in string form:

<pre><code>public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
                     throws IOException, InterruptedException {
    context.write(key, value);
  }
}
</code></pre>
