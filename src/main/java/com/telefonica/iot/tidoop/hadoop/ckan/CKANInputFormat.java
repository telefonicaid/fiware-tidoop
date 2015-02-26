/**
 * Copyright 2015 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-connectors (FI-WARE project).
 *
 * fiware-connectors is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-connectors is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

/**
 * Custom InputFormat for CKAN data.
 * 
 * @author frb
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CKANInputFormat extends InputFormat<LongWritable, Text> {
    
    private final Logger logger;
    public static final String CKAN_HOST = "mapreduce.input.ckaninputformat.host";
    public static final String CKAN_PORT = "mapreduce.input.ckaninputformat.port";
    public static final String CKAN_SSL = "mapreduce.input.ckaninputformat.ssl";
    public static final String CKAN_API_KEY = "mapreduce.input.ckaninputformat.apikey";
    public static final String INPUT_URLS = "mapreduce.input.ckaninputformat.inputurls";
    public static final String CKAN_SPLITS_LENGTH = "mapreduce.input.ckaninputformat.splitslength";
    private CKANBackend backend;

    /**
     * Constructor.
     */
    public CKANInputFormat() {
        logger = Logger.getLogger(CKANInputFormat.class);
        backend = null;
    } // CKANInputFormat
    
    /**
     * Sets the CKAN API environment.
     * @param job
     * @param ckanHost
     * @param ckanPort
     * @param ssl
     * @param ckanAPIKey
     */
    public static void setCKANEnvironmnet(Job job, String ckanHost, String ckanPort, boolean ssl, String ckanAPIKey) {
        job.getConfiguration().set(CKAN_HOST, ckanHost);
        job.getConfiguration().set(CKAN_PORT, ckanPort);
        job.getConfiguration().set(CKAN_SSL, ssl ? "true" : "false");
        job.getConfiguration().set(CKAN_API_KEY, ckanAPIKey);
    } // setCKANAPIKey
    
    /**
     * Sets the CKAN backend. It is protected since it is only used by the tests.
     * @param backend
     */
    protected void setCKANBackend(CKANBackend backend) {
        this.backend = backend;
    } // setCKANBackend
    
    /**
     * Adds new CKAN inputs. These can be comma-separated values.
     * @param job
     * @param ckanURL
     */
    public static void addCKANInput(Job job, String ckanURL) {
        Configuration conf = job.getConfiguration();
        String inputs = conf.get(INPUT_URLS, "");
        
        if (inputs.isEmpty()) {
            inputs += ckanURL;
        } else {
            inputs += "," + ckanURL;
        } // if else
        
        conf.set(INPUT_URLS, inputs);
    } // addCKANInput
    
    /**
     * Sets the CKAN splits length.
     * @param job
     * @param length
     */
    public static void setCKANSplitsLength(Job job, String length) {
        job.getConfiguration().set(CKAN_SPLITS_LENGTH, length);
    } // stCKANSplitsLength

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        // create a reader... it will need its own backend instace
        String ckanHost = context.getConfiguration().get(CKAN_HOST);
        String ckanPort = context.getConfiguration().get(CKAN_PORT);
        boolean ckanSSL = context.getConfiguration().get(CKAN_SSL).equals("true");
        String ckanAPIKey = context.getConfiguration().get(CKAN_API_KEY);
        logger.info("Creating record reader, the backend is at " + (ckanSSL ? "https://" : "http://") + ckanHost + ":"
                + ckanPort + " (API key=" + ckanAPIKey + ")");
        return new CKANRecordReader(new CKANBackend(ckanHost, ckanPort, ckanSSL, ckanAPIKey), split, context);
    } // createRecordReader
    
    @Override
    public List<InputSplit> getSplits(JobContext job) {
        // create a CKAN backend
        String ckanHost = job.getConfiguration().get(CKAN_HOST);
        String ckanPort = job.getConfiguration().get(CKAN_PORT);
        boolean ckanSSL = job.getConfiguration().get(CKAN_SSL).equals("true");
        String ckanAPIKey = job.getConfiguration().get(CKAN_API_KEY);
        logger.info("Getting splits, the backend is at " + (ckanSSL ? "https://" : "http://") + ckanHost + ":"
                + ckanPort + " (API key=" + ckanAPIKey + ")");
        
        if (backend == null) {
            backend = new CKANBackend(ckanHost, ckanPort, ckanSSL, ckanAPIKey);
        } // if
        
        // resulting splits container
        List<InputSplit> splits = new ArrayList<InputSplit>();
        
        // get the Job configuration
        Configuration conf = job.getConfiguration();
        
        // get the inputs, i.e. the list of CKAN URLs
        String input = conf.get(INPUT_URLS, "");
        String[] ckanURLs = StringUtils.split(input);
        
        // iterate on the CKAN URLs, they may be related to whole organizations, packages/datasets or specific resources
        for (String ckanURL: ckanURLs) {
            if (isCKANOrg(ckanURL)) {
                logger.info("Getting splits for " + ckanURL + ", it is an organization");
                splits.addAll(getSplitsOrg(ckanURL, job.getConfiguration()));
            } else if (isCKANPkg(ckanURL)) {
                logger.info("Getting splits for " + ckanURL + ", it is a package/dataset");
                splits.addAll(getSplitsPkg(ckanURL, job.getConfiguration()));
            } else {
                logger.info("Getting splits for " + ckanURL + ", it is a resource");
                splits.addAll(getSplitsRes(ckanURL, job.getConfiguration()));
            } // if else if
        } // for
        
        // return the splits
        logger.info("Number of total splits=" + splits.size());
        return splits;
    } // getSplits

    private boolean isCKANOrg(String ckanURL) {
        return ckanURL.contains("organization");
    } // isCKANOrg
    
    private boolean isCKANPkg(String ckanURL) {
        return ckanURL.contains("dataset") && !ckanURL.contains("resource");
    } // isCKANPkg
    
    private List<InputSplit> getSplitsOrg(String orgURL, Configuration conf) {
        if (backend == null) {
            logger.error("Unable to get the input splits, it seems the CKAN environment was not properly set");
            return null;
        } // if
        
        // get the organization identifier from the URL
        String[] urlParts = orgURL.split("/");
        String orgId = urlParts[urlParts.length - 1];
        
        // resulting splits container
        List<InputSplit> splits = new ArrayList<InputSplit>();
        
        // get all the organization packages
        List<String> pkgURLs = backend.getPackages(orgId);

        // for each package, get the splits for its resources
        for (String pkgURL : pkgURLs) {
            List<InputSplit> pkgSplits = getSplitsPkg(pkgURL, conf);
            splits.addAll(pkgSplits);
        } // for

        // return the splits
        return splits;
    } // getSplitsOrg
    
    private List<InputSplit> getSplitsPkg(String pkgURL, Configuration conf) {
        if (backend == null) {
            logger.error("Unable to get the input splits, it seems the CKAN environment was not properly set");
            return null;
        } // if
        
        // get the package identifier from the URL
        String[] urlParts = pkgURL.split("/");
        String pkgId = urlParts[urlParts.length - 1];
        
        // resulting splits container
        List<InputSplit> splits = new ArrayList<InputSplit>();

        // get all the package resources
        List<String> resIds = backend.getResources(pkgId);

        // for each package, get the splits for its resources
        for (String resId : resIds) {
            List<InputSplit> resSplits = getSplitsRes(resId, conf);
            splits.addAll(resSplits);
        } // for

        // return the splits
        return splits;
    } // getSplitsPkg
    
    private List<InputSplit> getSplitsRes(String resURL, Configuration conf) {
        if (backend == null) {
            logger.error("Unable to get the input splits, it seems the CKAN environment was not properly set");
            return null;
        } // if
        
        // get the resource identifier from the URL
        String[] urlParts = resURL.split("/");
        String resId = urlParts[urlParts.length - 1];
        
        // resulting splits container
        List<InputSplit> splits = new ArrayList<InputSplit>();
        
        // calculate the number of complete blocks
        int numRecords = backend.getNumRecords(resId);
        
        if (numRecords == 0) {
            return splits;
        } // if
        
        int splitsLength = new Integer(conf.get(CKAN_SPLITS_LENGTH));
        int numCompleteBlocks = numRecords / splitsLength;
        int i;
        
        // add a split for each complete block
        for (i = 0; i < numCompleteBlocks; i++) {
            splits.add(new CKANInputSplit(resId, i * splitsLength, splitsLength));
        } // for
        
        // add a split for the remaining records (uncomplete block)
        splits.add(new CKANInputSplit(resId, i * splitsLength, numRecords - (i * splitsLength)));
        
        // return the splits
        return splits;
    } // getSplitsRes

} // CKANInputFormat
