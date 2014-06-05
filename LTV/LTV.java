package com.deepforest.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.deepforest.avro.TrackingObject;
import java.util.*;

public class LTV extends Configured implements Tool {

  public static class LTVMapper extends
      Mapper<AvroKey<TrackingObject>, NullWritable, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    @Override
    public void map(AvroKey<TrackingObject> key, NullWritable value, Context context)
        throws IOException, InterruptedException {

      CharSequence app_id = key.datum().getAppId();
      if (app_id == null) app_id = "NULL";

      CharSequence network_id = key.datum().getNetworkId();
      if (network_id == null) network_id = "NULL";
      
      CharSequence ios_ifa = key.datum().getIosIfa();
      CharSequence android_id = key.datum().getAndroidId();
      CharSequence open_udid = key.datum().getOpenUdid();
      // TODO: get md5/sha1 IDs as well
      CharSequence device_id;
      if (ios_ifa != null)
        device_id = ios_ifa;
      else if (android_id != null)
        device_id = android_id;
      else if (open_udid != null)
        device_id = open_udid;
      else 
        device_id = "NULL";
      CharSequence event_name = key.datum().getEventName();
      if (event_name == null) event_name = "NULL";
      CharSequence event_type = key.datum().getEventType();
      if (event_type == null) event_type = "NULL";
      CharSequence event_annotation = key.datum().getEventAnnotation();
      if (event_annotation == null) event_annotation = "NULL";

      StringBuilder keysb = new StringBuilder();
      keysb.append(app_id.toString());
      keysb.append("\t");
      keysb.append(network_id.toString());
      keysb.append("\t");
      keysb.append(device_id.toString());
      keysb.append("\t");
      keysb.append(event_name.toString());
      keysb.append("\t");
      keysb.append(event_type.toString());
      keysb.append("\t");
      keysb.append(event_annotation.toString());
      outputKey.set(keysb.toString());

      

      CharSequence timestamp = key.datum().getTimestamp();
      if (timestamp == null) timestamp = "NULL";

      StringBuilder valuesb = new StringBuilder();
      //valuesb.append(device_id.toString());
      //valuesb.append("\t");
      valuesb.append(timestamp.toString());
      outputValue.set(valuesb.toString());

      context.write(outputKey, outputValue);
    }
  }

  public static class LTVReducer extends
      Reducer<Text, Text, Text, IntWritable> {
    private Text outputKey = new Text();
    private IntWritable outputValue = new IntWritable();
    //private Text outputValue = new Text();
    @Override
    public void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {

      // key is app_id+"\t"+network_id
      // values are device_id+"\t"+timestamp
      // split the key and the value to get each field

      outputKey.set(key);

      ArrayList<Integer> time_list = new ArrayList<Integer>();
      for (Text value : values) {
        String stringValue = value.toString();
        if (stringValue != "NULL")
            time_list.add(Integer.parseInt(stringValue));
      }
      Collections.sort(time_list);
      
      int digit24 = 0;
      int digit48 = 0;
      int digit72 = 0;
      //StringBuilder listResult = new StringBuilder();
      if (time_list.size() > 0) {
        int time24 = time_list.get(0) + 86400;
        int time48 = time_list.get(0) + 2 * 86400;
        int time72 = time_list.get(0) + 3 * 86400;
	
        for(int i = 1; i < time_list.size(); i++) {
            if (digit24 == 0 && time_list.get(i) > time24 && time_list.get(i) <= time48)
                digit24 = 1;
            else if (digit48 == 0 && time_list.get(i) > time48 && time_list.get(i) <= time72)
                digit48 = 1;
            else if (digit72 == 0 && time_list.get(i) > time72)
                digit72 = 1;
 
            //listResult.append(time_list.get(i).toString());
            //listResult.append("\t");
        }
      }
      int outValue = digit24 * 4 + digit48 * 2 + digit72;
      outputValue.set(outValue);
      //outputValue.set(listResult.toString());
      context.write(outputKey, outputValue);
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: LTV <input path> <output path>");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(LTV.class);
    job.setJobName("LTV");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapperClass(LTVMapper.class);
    AvroJob.setInputKeySchema(job, TrackingObject.getClassSchema());
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    job.setReducerClass(LTVReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setNumReduceTasks(1);

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new LTV(), args);
    System.exit(res);
  }
}
