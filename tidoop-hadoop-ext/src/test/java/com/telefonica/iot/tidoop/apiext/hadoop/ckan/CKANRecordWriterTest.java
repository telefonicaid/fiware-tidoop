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

import com.telefonica.iot.tidoop.apiext.hadoop.ckan.CKANRecordWriter;
import com.telefonica.iot.tidoop.apiext.backends.ckan.CKANBackend;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
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
public class CKANRecordWriterTest {
    
    // instance to be tested
    private CKANRecordWriter recordWriter;
    
    // mocks
    @Mock
    private CKANBackend backend;
    @Mock
    private TaskAttemptContext taskAttemptContext;
    
    // constants
    private final String jobName = "job-name";
    private final String resName = "res-name";
    private final String resId = "res-id";
    private final String pkgId = "pkg-id";
    private final Text key = new Text("key-to-be-written");
    private final IntWritable value = new IntWritable(12345);
    
    /**
     * Sets up tests by creating a unique instance of the tested class, and by defining the behaviour of the mocked
     * classes.
     *  
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        // set up the instance of the tested class
        recordWriter = new CKANRecordWriter(backend, pkgId, taskAttemptContext);
        
        // set up the behaviour of the mocked classes
        when(backend.createResource(resName, pkgId)).thenReturn(resId);
        when(taskAttemptContext.getJobName()).thenReturn(jobName);
    } // setUp
    
    /**
     * Test of write method, of class CKANRecordWriter.
     */
    @Test
    public void testWrite() {
        System.out.println("Testing CKANRecordWriter.write");
        try {
            recordWriter.write(key, value);
        } catch (IOException e) {
            fail(e.getMessage());
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } finally {
            assertTrue(true);
        } // try catch finally
    } // testWrite
    
    /**
     * Test of close method, of class CKANRecordWriter.
     */
    @Test
    public void testClose() {
        System.out.println("Testing CKANRecordWriter.close");
        assertTrue(true);
    } // testClose
    
} // CKANRecordWriterTest
