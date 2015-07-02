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
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author frb
 */
public class PlainJSONToCSV extends Configured implements Tool {
    
    /**
     * Mapper class.
     */
    public static class LineConverter extends Mapper<Object, Text, Text, Text> {
        
        private String separatorValue;
        private final Text commonKey = new Text("common-key");

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            separatorValue = context.getConfiguration().get(Constants.PARAM_SEPARATOR_VALUE, ",");
        } // setup

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONParser jsonParser = new JSONParser();
            
            try {
                JSONObject o = (JSONObject) jsonParser.parse(value.toString());
                String csvLine = "";
                Iterator it = o.keySet().iterator();
                
                if (it.hasNext()) {
                    csvLine += (String) o.get((String) it.next());
                    
                    while (it.hasNext()) {
                        csvLine += separatorValue + (String) o.get((String) it.next());
                    } // while
                } // if

                context.write(commonKey, new Text(csvLine));
            } catch (ParseException e) {
                context.write(commonKey, null);
            } // try catch
        } // map
        
    } // LineConverter
    
    /**
     * Combiner class. It implements the same code than LinesJoiner, except for the emitted (k,v) pair: in the
     * reducer the emitted pair is about (NullWritable,Text) but the combiner must emit the pairs as if they were
     * emitted by the mapper, thus is about emitting (Text,Text) pairs.
     */
    public static class LinesCombiner extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> filteredLines, Context context)
            throws IOException, InterruptedException {
            for (Text filteredLine : filteredLines) {
                context.write(key, filteredLine);
            } // for
        } // reduce
        
    } // LinesCombiner

    /**
     * Reducer class.
     */
    public static class LinesJoiner extends Reducer<Text, Text, NullWritable, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> filteredLines, Context context)
            throws IOException, InterruptedException {
            for (Text filteredLine : filteredLines) {
                context.write(NullWritable.get(), filteredLine);
            } // for
        } // reduce
        
    } // LinesJoiner
    
    /**
     * Main class.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PlainJSONToCSV(), args);
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
        String separatorValue = args[2];
        
        // create and configure a MapReduce job
        Configuration conf = this.getConf();
        conf.set(Constants.PARAM_SEPARATOR_VALUE, separatorValue);
        Job job = Job.getInstance(conf, "tidoop-mr-lib-ngsitocsv");
        job.setNumReduceTasks(1);
        job.setJarByClass(PlainJSONToCSV.class);
        job.setMapperClass(LineConverter.class);
        job.setCombinerClass(LinesCombiner.class);
        job.setReducerClass(LinesJoiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
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
        System.out.println("   com.telefonica.iot.tidoop.mrlib.ngsi.NGSIToCSV \\");
        System.out.println("   -libjars target/tidoop-mr-lib-x.y.z-jar-with-dependencies.jar \\");
        System.out.println("   <HDFS_input> \\");
        System.out.println("   <HDFS_output> \\");
        System.out.println("   <separator_value>");
    } // showUsage
    
} // PlainJSONToCSV
