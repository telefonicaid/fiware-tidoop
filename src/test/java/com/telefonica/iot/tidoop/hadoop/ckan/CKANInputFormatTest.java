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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
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
public class CKANInputFormatTest {
    
    // instance to be tested
    private CKANInputFormat ckanInputFormat;
    
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
    private final String ckanInputRes = "https://data.lab.fiware.org/dataset/example_dataset/resource/1234567890";
    private final String ckanInputPkg = "https://data.lab.fiware.org/dataset/example_dataset/";
    private final String ckanInputOrg = "https://data.lab.fiware.org/organization/example_organization";
    private final String resId = "1234567890";
    private final String splitsLength = "1000";
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
        ckanInputFormat = new CKANInputFormat();
        
        // set up the other instances
        conf = new Configuration();
        job = Job.getInstance(conf, "testGetSplitsResource");
        
        // set up the behavious of the mocked classes
        when(backend.getNumRecords(resId)).thenReturn(numRecords);
    } // setUp
    
    /**
     * Test of setEnvironment method, of class CKANInputFormat.
     */
    @Test
    public void testSetCKANEnvironmnet() {
        System.out.println("Testing CKANInputFormat.setCKANEnvironmnet");
        
        try {
            CKANInputFormat.setEnvironment(job, ckanHost, ckanPort, enableSSL, ckanAPIKey);
            assertTrue(true);
        } catch (Exception e) {
            fail(e.getMessage());
        } // try catch
    } // testSetCKANEnvironmnet
    
    /**
     * Test of setInput method, of class CKANInputFormat.
     */
    @Test
    public void testAddCKANInput() {
        System.out.println("Testing CKANInputFormat.addCKANInput");
        
        try {
            CKANInputFormat.setInput(job, ckanInputRes);
            assertTrue(true);
        } catch (Exception e) {
            fail(e.getMessage());
        } // try catch
    } // testAddCKANInput
    
    /**
     * Test of setSplitsLength method, of class CKANInputFormat.
     */
    @Test
    public void testSetCKANSplitsLength() {
        System.out.println("Testing CKANInputFormat.addCKANInput");
        
        try {
            CKANInputFormat.setSplitsLength(job, splitsLength);
            assertTrue(true);
        } catch (Exception e) {
            fail(e.getMessage());
        } // try catch
    } // testSetCKANSplitsLength

    /**
     * Test of createRecordReader method, of class CKANInputFormat.
     */
    @Test
    public void testCreateRecordReader() {
        System.out.println("Testing CKANInputFormat.createRecordReader)");
        job.setInputFormatClass(CKANInputFormat.class);
        CKANInputFormat.setEnvironment(job, ckanHost, ckanPort, enableSSL, ckanAPIKey);
        CKANInputFormat.setInput(job, ckanInputRes);
        CKANInputFormat.setSplitsLength(job, splitsLength);
        CKANInputFormat inputFormat = new CKANInputFormat();
        RecordReader recordReader = inputFormat.createRecordReader(
                new CKANInputSplit(resId, 0, 1000),
                new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID(), null));
        assertTrue(recordReader != null);
    } // testCreateRecordReader
    
    /**
     * Test of getSplits method, of class CKANInputFormat.
     */
    @Test
    public void testGetSplits() {
        System.out.println("Testing CKANInputFormat.getSplits (resource)");
        job.setInputFormatClass(CKANInputFormat.class);
        CKANInputFormat.setEnvironment(job, ckanHost, ckanPort, enableSSL, ckanAPIKey);
        CKANInputFormat.setInput(job, ckanInputRes);
        CKANInputFormat.setSplitsLength(job, splitsLength);
        CKANInputFormat inputFormat = new CKANInputFormat();
        inputFormat.setCKANBackend(backend);
        List<InputSplit> splits = inputFormat.getSplits(job);
        assertTrue(splits.size() > 0);

        System.out.println("Testing CKANInputFormat.getSplits (package)");
        job.setInputFormatClass(CKANInputFormat.class);
        CKANInputFormat.setEnvironment(job, ckanHost, ckanPort, enableSSL, ckanAPIKey);
        CKANInputFormat.setInput(job, ckanInputPkg);
        CKANInputFormat.setSplitsLength(job, splitsLength);
        inputFormat = new CKANInputFormat();
        inputFormat.setCKANBackend(backend);
        splits = inputFormat.getSplits(job);
        assertTrue(splits.size() > 0);
        
        System.out.println("Testing CKANInputFormat.getSplits (organization)");
        job.setInputFormatClass(CKANInputFormat.class);
        CKANInputFormat.setEnvironment(job, ckanHost, ckanPort, enableSSL, ckanAPIKey);
        CKANInputFormat.setInput(job, ckanInputOrg);
        CKANInputFormat.setSplitsLength(job, splitsLength);
        inputFormat = new CKANInputFormat();
        inputFormat.setCKANBackend(backend);
        splits = inputFormat.getSplits(job);
        assertTrue(splits.size() > 0);
    } // testGetSplits
    
} // CKANInputFormatTest
