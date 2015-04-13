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
 * You should have received a copy of the GNU Affero General Public License along with fiware-connectors. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with
 * francisco.romerobueno at telefonica dot com
 */

package com.telefonica.iot.tidoop.hadoop.ckan;

import com.telefonica.iot.tidoop.backends.ckan.CKANBackend;
import static com.telefonica.iot.tidoop.utils.Constants.OUTPUT_CKAN_API_KEY;
import static com.telefonica.iot.tidoop.utils.Constants.OUTPUT_CKAN_HOST;
import static com.telefonica.iot.tidoop.utils.Constants.OUTPUT_CKAN_PORT;
import static com.telefonica.iot.tidoop.utils.Constants.OUTPUT_CKAN_SSL;
import static com.telefonica.iot.tidoop.utils.Constants.OUTPUT_CKAN_URL;
import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 * Custom OutputFormat for CKAN data.
 * 
 * @author frb
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CKANOutputFormat extends OutputFormat<Text, IntWritable> {

    private final Logger logger;
    
    /**
     * Constructor.
     */
    public CKANOutputFormat() {
        logger = Logger.getLogger(CKANOutputFormat.class);
    } // CKANOutputFormat
    
    /**
     * Sets the CKAN API environment.
     * @param job
     * @param ckanHost
     * @param ckanPort
     * @param ssl
     * @param ckanAPIKey
     */
    public static void setEnvironmnet(Job job, String ckanHost, String ckanPort, boolean ssl, String ckanAPIKey) {
        job.getConfiguration().set(OUTPUT_CKAN_HOST, ckanHost);
        job.getConfiguration().set(OUTPUT_CKAN_PORT, ckanPort);
        job.getConfiguration().set(OUTPUT_CKAN_SSL, ssl ? "true" : "false");
        job.getConfiguration().set(OUTPUT_CKAN_API_KEY, ckanAPIKey);
    } // setCKANAPIKey
    
    /**
     * Sets the output package given a URL.
     * 
     * @param job Job which output will be set
     * @param pkgURL Output package in URL form
     */
    public static void setOutputPkg(Job job, String pkgURL) {
        Configuration conf = job.getConfiguration();
        conf.set(OUTPUT_CKAN_URL, pkgURL);
    } // setOutputPkg
    
    @Override
    public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext context)
        throws IOException, InterruptedException {
        // get the package identifier from the URL
        String pkgURL = context.getConfiguration().get(OUTPUT_CKAN_URL);
        String[] urlParts = pkgURL.split("/");
        String pkgId = urlParts[urlParts.length - 1];
        
        // create a reader... it will need its own backend instace
        String ckanHost = context.getConfiguration().get(OUTPUT_CKAN_HOST);
        String ckanPort = context.getConfiguration().get(OUTPUT_CKAN_PORT);
        boolean ckanSSL = context.getConfiguration().get(OUTPUT_CKAN_SSL).equals("true");
        String ckanAPIKey = context.getConfiguration().get(OUTPUT_CKAN_API_KEY);
        logger.info("Creating record reader, the backend is at " + (ckanSSL ? "https://" : "http://") + ckanHost + ":"
                + ckanPort + " (API key=" + ckanAPIKey + ")");
        return new CKANRecordWriter(new CKANBackend(ckanHost, ckanPort, ckanSSL, ckanAPIKey, 0), pkgId, context);
    } // getRecordWriter

    @Override
    public void checkOutputSpecs(JobContext jc) throws IOException, InterruptedException {
        // this method checks if the final org/pkg/res element exists or not...
        // if the organization does not exist, it is created
        // if the package does not exist, it is created
        // if the resource exists, an error is given
        //throw new UnsupportedOperationException("Not supported yet.");
        logger.info("Checking output specs... everything OK");
    } // checkOutputSpecs

    @Override
    public synchronized CKANOutputCommitter getOutputCommitter(TaskAttemptContext tac)
        throws IOException, InterruptedException {
        return new CKANOutputCommitter();
    } // getOutputCommitter
    
} // CKANOutputFormat
