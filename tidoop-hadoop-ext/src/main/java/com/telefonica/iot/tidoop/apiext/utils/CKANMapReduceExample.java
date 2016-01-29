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
package com.telefonica.iot.tidoop.apiext.utils;

import com.telefonica.iot.tidoop.apiext.hadoop.ckan.CKANInputFormat;
import com.telefonica.iot.tidoop.apiext.hadoop.ckan.CKANOutputFormat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author frb
 */
public final class CKANMapReduceExample extends Configured implements Tool {
    
    /**
     * Constructor. It is private since utility classes should not have a public or default constructor.
     */
    private CKANMapReduceExample() {
    } // CKANMapReduceTest

    /**
     * Mapper class. It receives a CKAN record pair (Object key, Text ckanRecord) and returns a (Text globalKey,
     * IntWritable recordLength) pair about a common global key and the length of the record.
     */
    public static class RecordSizeGetter extends Mapper<Object, Text, Text, IntWritable> {
        
        private final Text globalKey = new Text("size");
        private final IntWritable recordLength = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            recordLength.set(value.getLength());
            context.write(globalKey, recordLength);
        } // map
        
    } // RecordSizeGetter

    /**
     * Reducer class. It receives a list of (Text globalKey, IntWritable length) pairs and computes the sum of all the
     * lengths, producing a final (Text globalKey, IntWritable totalLength).
     */
    public static class RecordSizeAdder extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private final IntWritable totalLength = new IntWritable();

        @Override
        public void reduce(Text globalKey, Iterable<IntWritable> recordLengths, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            
            for (IntWritable val : recordLengths) {
                sum += val.get();
            } // for
            
            totalLength.set(sum);
            context.write(globalKey, totalLength);
        } // reduce
        
    } // RecordSizeAdder

    /**
     * Main class.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CKANMapReduceExample(), args);
        System.exit(res);
    } // main
    
    @Override
    public int run(String[] args) throws Exception {
        // check the number of arguments, show the usage if it is wrong
        if (args.length != 7) {
            showUsage();
            return -1;
        } // if
        
        // get the arguments
        String ckanHost = args[0];
        String ckanPort = args[1];
        boolean sslEnabled = args[2].equals("true");
        String ckanAPIKey = args[3];
        String ckanInputs = args[4];
        String ckanOutput = args[5];
        String splitsLength = args[6];
        
        // create and configure a MapReduce job
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "CKAN MapReduce test");
        job.setJarByClass(CKANMapReduceExample.class);
        job.setMapperClass(RecordSizeGetter.class);
        job.setCombinerClass(RecordSizeAdder.class);
        job.setReducerClass(RecordSizeAdder.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(CKANInputFormat.class);
        CKANInputFormat.setInput(job, ckanInputs);
        CKANInputFormat.setEnvironment(job, ckanHost, ckanPort, sslEnabled, ckanAPIKey);
        CKANInputFormat.setSplitsLength(job, splitsLength);
        job.setOutputFormatClass(CKANOutputFormat.class);
        CKANOutputFormat.setEnvironment(job, ckanHost, ckanPort, sslEnabled, ckanAPIKey);
        CKANOutputFormat.setOutputPkg(job, ckanOutput);
        
        // run the MapReduce job
        return job.waitForCompletion(true) ? 0 : 1;
    } // main
    
    private void showUsage() {
        System.out.println("Usage:");
        System.out.println();
        System.out.println("hadoop jar \\");
        System.out.println("   target/ckan-protocol-0.1-jar-with-dependencies.jar \\");
        System.out.println("   es.tid.fiware.fiwareconnectors.ckanprotocol.utils.CKANMapReduceTest \\");
        System.out.println("   -libjars target/ckan-protocol-0.1-jar-with-dependencies.jar \\");
        System.out.println("   <ckan host> \\");
        System.out.println("   <ckan port> \\");
        System.out.println("   <ssl enabled=true|false> \\");
        System.out.println("   <ckan API key> \\");
        System.out.println("   <comma-separated list of ckan inputs> \\");
        System.out.println("   <ckan output package> \\");
        System.out.println("   <splits length>");
    } // showUsage
    
} // CKANMapReduceTest
