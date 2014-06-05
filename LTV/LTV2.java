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

            CharSequence site_id = key.datum().getSiteId();
            if (site_id == null) site_id = "NULL";

            StringBuilder keysb = new StringBuilder();
            keysb.append(app_id.toString());
            keysb.append("\t");
            keysb.append(device_id.toString());
            outputKey.set(keysb.toString());

            CharSequence timestamp = key.datum().getTimestamp();
            if (timestamp == null) timestamp = "NULL";

            StringBuilder valuesb = new StringBuilder();
            valuesb.append(timestamp.toString());
            valuesb.append("\t");
            valuesb.append(event_name.toString());
            valuesb.append("\t");
            valuesb.append(event_type.toString());
            valuesb.append("\t");
            valuesb.append(event_annotation.toString());
            valuesb.append("\t");
            valuesb.append(network_id.toString());
            valuesb.append("\t");
            valuesb.append(site_id.toString());
            outputValue.set(valuesb.toString());

            context.write(outputKey, outputValue);
        }
    }

  private static int dateTimeStamp = 0;
  public static class LTVReducer extends
      Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        //private IntWritable outputValue = new IntWritable();
        private Text outputValue = new Text();
        @Override
        public void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {

                // key is app_id+"\t"+network_id
                // values are device_id+"\t"+timestamp
                // split the key and the value to get each field

                outputKey.set(key);

                ArrayList<String> time_list = new ArrayList<String>();
                int installTime = 0;
                String installNetworkId = "NULL";
                String installSiteId = "NULL";
                for (Text value : values) {
                    String stringValue = value.toString();
                    if (stringValue != "NULL") {
                        String[] subStringValue = stringValue.split("\t");
                            // 0 timestamp 1 event name 2 event type 3 event annotation
                            // 4 network id 5 site id
                        String eventAnnotation = subStringValue[3];
                        String eventName = subStringValue[1];
                        String networkId = subStringValue[4];
                        String siteId = subStringValue[5];
                        if (eventAnnotation.equals("install")) {
                            installTime = Integer.parseInt(subStringValue[0]);
                            //if (installTime - dateTimeStamp >= 86400) {
                            //   installTime = 0;  //discard the event if install is not from the first day
                            //} else {
                                installNetworkId = subStringValue[4];
                                installSiteId = subStringValue[5];
                            //}
                        } else {    
                            time_list.add(subStringValue[0] + "\t" + subStringValue[4] + "\t" + subStringValue[5] ); 
                        }
                    }
                }
    
                if (installTime != 0) {
                    Collections.sort(time_list);
                    //Collections.sort(time_list, new CustomComparator());
      
                    int digit24 = 0;
                    int digit48 = 0;
                    int digit72 = 0;
                    //StringBuilder listResult = new StringBuilder();
                    if (time_list.size() > 0) {
                        int time24 = installTime + 86400;
                        int time48 = installTime + 2 * 86400;
                        int time72 = installTime + 3 * 86400;
    
                        for(int i = 0; i < time_list.size(); i++) {
			    String[] subStringValue = time_list.get(i).split("\t");
			    if (subStringValue[1].equals(installNetworkId) && subStringValue[2].equals(installSiteId)) {
				if (digit24 == 0 && Integer.parseInt(time_list.get(i).substring(0, 10)) > time24 && Integer.parseInt(time_list.get(i).substring(0, 10)) <= time48)
                                digit24 = 1;
                            else if (digit48 == 0 && Integer.parseInt(time_list.get(i).substring(0, 10)) > time48 && Integer.parseInt(time_list.get(i).substring(0, 10)) <= time72)
                                digit48 = 1;
                            else if (digit72 == 0 && Integer.parseInt(time_list.get(i).substring(0, 10)) > time72)
                                digit72 = 1;
			    }
                            
 
                            //listResult.append(time_list.get(i).toString());
                            //listResult.append("\t");
                        }
                    }
                int outValue = digit24 * 4 + digit48 * 2 + digit72;
                //outputValue.set(outValue);
               // String result = installNetworkId + "\t" + installSiteId + "\t" + Integer.toString(outValue);
                String result = installNetworkId + "\t" + installSiteId + "\t" +Integer.toString(digit24) + "\t" + Integer.toString(digit48) + "\t" + Integer.toString(digit72);
                if (outValue > 0) {
		    outputValue.set(result);
                    context.write(outputKey, outputValue);
		}
                }
      
            }
    }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: LTV <input path> <output path>");
      return -1;
    }

    dateTimeStamp = 1400716800; // manual input timestamp of the data 05 / 22 / 2014  0:0:0
    
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
