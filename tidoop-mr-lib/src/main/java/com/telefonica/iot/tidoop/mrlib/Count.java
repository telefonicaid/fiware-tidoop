/**
 * Copyright 2015 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-tidoop (FI-WARE project).
 *
 * fiware-tidoop is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-tidoop is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-tidoop. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with
 * francisco.romerobueno at telefonica dot com
 */
package com.telefonica.iot.tidoop.mrlib;

import com.telefonica.iot.tidoop.mrlib.utils.Constants;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 *
 * @author frb
 */
public class Count extends Configured implements Tool {
    
    private static final Logger LOGGER = Logger.getLogger(Count.class);
    
    /**
     * Mapper class.
     */
    public static class UnitEmitter extends Mapper<Object, Text, Text, LongWritable> {
        
        private final Text commonKey = new Text("common-key");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(commonKey, new LongWritable(1));
        } // map
        
    } // UnitEmitter
    
    /**
     * Combiner class. It implements the same code than the reducer class, except for the emitted (k,v) pair: in the
     * reducer the emitted pair is about (NullWritable,Text) but the combiner must emit the pairs as if they were
     * emitted by the mapper, thus is about emitting (Text,Text) pairs.
     */
    public static class Adder extends Reducer<Text, LongWritable, Text, LongWritable> {
        
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
            long sum = 0;
            
            for (LongWritable value : values) {
                sum += value.get();
            } // for
            
            context.write(key, new LongWritable(sum));
        } // reduce
        
    } // Adder
    
    /**
     * Reducer class.
     */
    public static class AdderWithTag extends Reducer<Text, LongWritable, Text, LongWritable> {
        
        private String tag;
        
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            tag = context.getConfiguration().get(Constants.PARAM_TAG, "");
        } // setup

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
            long sum = 0;
            
            for (LongWritable value : values) {
                sum += value.get();
            } // for
            
            context.write(tag == null || tag.length() == 0 ? null : new Text(tag), new LongWritable(sum));
        } // reduce
        
    } // AdderWithTag
    
    /**
     * Main class.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Filter(), args);
        System.exit(res);
    } // main
    
    @Override
    public int run(String[] args) throws Exception {
        // check the number of arguments, show the usage if it is wrong
        if (args.length != 3) {
            showUsage();
            return -1;
        } // if
        
        // get the arguments
        String input = args[0];
        String output = args[1];
        String tag = args[2];
        
        // create and configure a MapReduce job
        Configuration conf = this.getConf();
        conf.set(Constants.PARAM_TAG, tag);
        Job job = Job.getInstance(conf, "tidoop-mr-lib-count");
        job.setNumReduceTasks(1);
        job.setJarByClass(Count.class);
        job.setMapperClass(UnitEmitter.class);
        job.setCombinerClass(Adder.class);
        job.setReducerClass(AdderWithTag.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        // run the MapReduce job
        return job.waitForCompletion(true) ? 0 : 1;
    } // main
    
    private void showUsage() {
        System.out.println("Usage:");
        System.out.println();
        System.out.println("hadoop jar \\");
        System.out.println("   target/tidoop-mr-lib-x.y.z-jar-with-dependencies.jar \\");
        System.out.println("   com.telefonica.iot.tidoop.mrlib.Count \\");
        System.out.println("   -libjars target/tidoop-mr-lib-x.y.z-jar-with-dependencies.jar \\");
        System.out.println("   <HDFS_input> \\");
        System.out.println("   <HDFS_output> \\");
        System.out.println("   <tag>");
    } // showUsage
    
} // Count
