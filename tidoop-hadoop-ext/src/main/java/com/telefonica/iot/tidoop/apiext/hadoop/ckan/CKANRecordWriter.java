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
package com.telefonica.iot.tidoop.apiext.hadoop.ckan;

import com.telefonica.iot.tidoop.apiext.backends.ckan.CKANBackend;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 * Custom RecordWriter for CKAN data.
 * 
 * @author frb
 */
public class CKANRecordWriter extends RecordWriter<Text, IntWritable> {
    
    private final Logger logger;
    private final CKANBackend backend;
    private final String pkgId;
    private final TaskAttemptContext context;
    
    /**
     * Constructor.
     * 
     * @param backend
     * @param pkgId CKAN package/dataset id or name where the output data will be written.
     * @param context
     */
    public CKANRecordWriter(CKANBackend backend, String pkgId, TaskAttemptContext context) {
        this.logger = Logger.getLogger(CKANRecordWriter.class);
        this.backend = backend;
        this.pkgId = pkgId;
        this.context = context;
    } // CKANRecordWriter

    @Override
    public void write(Text key, IntWritable value) throws IOException, InterruptedException {
        String resName = createResName(context.getJobName());
        
        try {
            String resId = backend.createResource(resName, pkgId);
            backend.createKeyValueDatastore(resId);
            backend.insertKeyValue(resId, key, value);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } // try catch
    } // write

    @Override
    public void close(TaskAttemptContext tac) throws IOException, InterruptedException {
        // nothig to close, using the CKAN REST API there is no resource "opening" at all
    } // close
    
    /**
     * Creates a resource name based on the job name and the current date. It is protected in order it can be accessed
     * from the tests.
     * @param jobName
     * @return
     */
    private String createResName(String jobName) {
        return new Date().getTime() + "_" + jobName.replaceAll(" ", "-");
    } // createResName
    
} // CKANRecordReader
