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
package com.telefonica.iot.tidoop.hadoop.ckan;

import com.telefonica.iot.tidoop.backends.ckan.CKANBackend;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author frb
 */
@RunWith(MockitoJUnitRunner.class)
public class CKANOutputFormatTest {
    
    // instance to be tested
    private CKANOutputFormat ckanOutputFormat;
    
    // other instances
    private Configuration conf;
    private Job job;
    
    // mocks
    @Mock
    private CKANBackend backend;
    
    // constants
    private final String ckanHost = "data.lab.fi-ware.org";
    private final String ckanPort = "80";
    private final boolean enableSSL = false;
    private final String ckanAPIKey = "ckan_api_key_example";
    private final String ckanOutputRes = "https://data.lab.fiware.org/dataset/example_dataset";
    private final String resId = "1234567890";
    private final int numRecords = 7409;
    
    /**
     * Sets up tests by creating a unique instance of the tested class, and by defining the behaviour of the mocked
     * classes.
     *  
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        // set up the instance of the tested class
        ckanOutputFormat = new CKANOutputFormat();
        
        // set up the other instances
        conf = new Configuration();
        job = Job.getInstance(conf, "CKANOutputFormatTest");
        
        // set up the behavious of the mocked classes
        when(backend.getNumRecords(resId)).thenReturn(numRecords);
    } // setUp
    
    /**
     * Test of setEnvironment method, of class CKANOutputFormat.
     */
    @Test
    public void testSetEnvironmnet() {
        System.out.println("Testing CKANOutputFormat.setEnvironmnet");
        
        try {
            CKANOutputFormat.setEnvironment(job, ckanHost, ckanPort, enableSSL, ckanAPIKey);
            assertTrue(true);
        } catch (Exception e) {
            fail(e.getMessage());
        } // try catch
    } // testSetEnvironmnet
    
    /**
     * Test of addCKANInput method, of class CKANInputFormat.
     */
    @Test
    public void testSetOutputPkg() {
        System.out.println("Testing CKANOutputFormat.setCKANOutput");
        
        try {
            CKANOutputFormat.setOutputPkg(job, ckanOutputRes);
            assertTrue(true);
        } catch (Exception e) {
            fail(e.getMessage());
        } // try catch
    } // testSetOutputPkg
    
    /**
     * Test of getRecordWriter method, of class CKANOutputFormat.
     */
    @Test
    public void testGetRecordWriter() {
        System.out.println("Testing CKANOutputFormat.getRecordWriter)");
        job.setOutputFormatClass(CKANOutputFormat.class);
        CKANOutputFormat.setEnvironment(job, ckanHost, ckanPort, enableSSL, ckanAPIKey);
        CKANOutputFormat.setOutputPkg(job, ckanOutputRes);
        CKANOutputFormat outputFormat = new CKANOutputFormat();
        RecordWriter recordWriter = null;
        
        try {
            recordWriter = outputFormat.getRecordWriter(
                    new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID(), null));
        } catch (IOException e) {
            fail(e.getMessage());
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } // try catch
        
        assertTrue(recordWriter != null);
    } // testGetRecordWriter
    
    /**
     * Test of checkOutputSpecs method, of class CKANOutputFormat.
     */
    @Test
    public void testCheckOutputSpecs() {
        System.out.println("Testing CKANOutputFormat.checkOutputSpecs");
        
        try {
            CKANOutputFormat outputFormat = new CKANOutputFormat();
            outputFormat.getOutputCommitter(
                    new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID(), null));
            assertTrue(true);
        } catch (IOException e) {
            fail(e.getMessage());
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } // try catch
    } // testCheckOutputSpecs
    
    /**
     * Test of getOutputCommitter method, of class CKANOutputFormat.
     */
    @Test
    public void testGetOutputCommitter() {
        System.out.println("Testing CKANOutputFormat.getOutputCommitter");
        
        try {
            CKANOutputFormat outputFormat = new CKANOutputFormat();
            outputFormat.getOutputCommitter(
                    new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID(), null));
            assertTrue(true);
        } catch (IOException e) {
            fail(e.getMessage());
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } // try catch
    } // testGetOutputCommitter
    
} // CKANOutputFormatTest
