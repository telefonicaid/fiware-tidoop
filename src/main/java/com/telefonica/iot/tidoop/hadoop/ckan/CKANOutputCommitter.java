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

import java.io.IOException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 * Custom OutputCommitter for CKAN data.
 * 
 * Useful link about what is an OutputCommitter:
 * http://johnjianfang.blogspot.com.es/2014/09/outputcommitter-in-hadoop-two.html
 * 
 * @author frb
 */
public class CKANOutputCommitter extends OutputCommitter {
    
    private final Logger logger;
    
    /**
     * Constructor.
     */
    public CKANOutputCommitter() {
        this.logger = Logger.getLogger(CKANOutputCommitter.class);
    } // CKANOutputCommitter

    @Override
    public void setupJob(JobContext jc) throws IOException {
        logger.debug("Nothing to setup since the outputs are written directly");
    } // setupJob

    @Override
    public void setupTask(TaskAttemptContext tac) throws IOException {
        logger.debug("Nothing to setup since the outputs are written directly");
    } // setupTask

    @Override
    public boolean needsTaskCommit(TaskAttemptContext tac) throws IOException {
        return false;
    } // needsTaskCommit

    @Override
    public void commitTask(TaskAttemptContext tac) throws IOException {
        logger.debug("Nothing to commit since the outputs are written directly");
    } // commitTask

    @Override
    public void abortTask(TaskAttemptContext tac) throws IOException {
        logger.debug("Nothing to abort since the outputs are written directly");
    } // abortTask
    
} // CKANOutputCommitter
