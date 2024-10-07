import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class inverse {

 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        int idx = -1 ;
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if(idx == -1){
                idx = Integer.parseInt(token);
            }
            else{
                context.write(new Text(token), new IntWritable(idx));
            }
        }
    }
 }

 public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
        Set<Integer> sites = new HashSet<>();
        for (IntWritable val : values) {
            sites.add(val.get());
        }
        ArrayList<Integer> sortedSites = new ArrayList<>(sites);
        Collections.sort(sortedSites);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < sortedSites.size(); i++) {
            sb.append(sortedSites.get(i));
            if (i < sortedSites.size() - 1) {
                sb.append(",");
            }
        }
        context.write(key, new Text(sb.toString()));
    }
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcount");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setJarByClass(WordCount.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
 }

}