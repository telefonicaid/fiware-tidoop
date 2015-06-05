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

import bsh.EvalError;
import bsh.Interpreter;
import com.telefonica.iot.tidoop.mrlib.utils.Constants;
import java.io.IOException;;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author frb
 */
public class MapOnly extends Configured implements Tool {
    
    private final static Logger logger = Logger.getLogger(MapOnly.class);
    
    /**
     * Mapper class.
     */
    public static class CustomMapper extends Mapper<Object, Text, NullWritable, Object> {
        
        private Interpreter interpreter = null;
        private String mapFunction = null;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // create a beanshell interpreter for the map function
            interpreter = new Interpreter();
            mapFunction = context.getConfiguration().get(Constants.PARAM_FUNCTION, "");
        } // setup

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                interpreter.set("x", value);
                interpreter.eval(mapFunction);
                Object y = interpreter.get("y");
                context.write(NullWritable.get(), y);
            } catch (EvalError e) {
                logger.error("Error while evaluating the mapping function: " + e.getMessage());
            } // try catch
        } // map
        
    } // CustomMapper
    
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
        String mapFunction = args[2];
        
        // create and configure a MapReduce job
        Configuration conf = this.getConf();
        conf.set(Constants.PARAM_FUNCTION, mapFunction);
        Job job = Job.getInstance(conf, "tidoop-mr-lib-maponly");
        job.setNumReduceTasks(0);
        job.setJarByClass(Filter.class);
        job.setMapperClass(Filter.LineFilter.class);
        job.setCombinerClass(Filter.LinesCombiner.class);
        job.setReducerClass(Filter.LinesJoiner.class);
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
        System.out.println("   com.telefonica.iot.tidoop.mrlib.MapOnly \\");
        System.out.println("   -libjars target/tidoop-mr-lib-x.y.z-jar-with-dependencies.jar \\");
        System.out.println("   <HDFS input> \\");
        System.out.println("   <HDFS output> \\");
        System.out.println("   <mapFunction>");
    } // showUsage
    
} // MapOnly
