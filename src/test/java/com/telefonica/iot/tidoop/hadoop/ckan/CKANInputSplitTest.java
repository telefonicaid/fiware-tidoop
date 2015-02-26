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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author frb
 */
@RunWith(MockitoJUnitRunner.class)
public class CKANInputSplitTest {
    
    // instance to be tested
    private CKANInputSplit ckanInputSplit;
    
    // mocks
    @Mock
    private DataInput dataInput;
    @Mock
    private DataOutput dataOutput;
    
    // constants
    private final String resId = "res_id_example";
    private final long firstRecordIndex = 0;
    private final long length = 1000;
    
    /**
     * Sets up tests by creating a unique instance of the tested class, and by defining the behaviour of the mocked
     * classes.
     *  
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        // set up the instance of the tested class
        ckanInputSplit = new CKANInputSplit(resId, firstRecordIndex, length);
        
        // set up the behaviour of the mocked classes
        when(dataInput.readLong()).thenReturn(firstRecordIndex);
        doNothing().when(dataOutput).writeLong(length);
    } // setUp
    
    /**
     * Test of getLength method, of class CKANInputSplit.
     */
    @Test
    public void testGetLength() {
        System.out.println("Testing CKANInputSplit.getLength");
        
        try {
            assertEquals(1000, ckanInputSplit.getLength());
        } catch (IOException e) {
            fail(e.getMessage());
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } // try catch
    } // testGetLength
    
    /**
     * Test of getLocations method, of class CKANInputSplit.
     */
    @Test
    public void testGetLocations() {
        System.out.println("Testing CKANInputSplit.getLocations");
        
        try {
            assertEquals(0, ckanInputSplit.getLocations().length);
        } catch (IOException e) {
            fail(e.getMessage());
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } // try catch
    } // testGetLocations
    
    /**
     * Test of getResId method, of class CKANInputSplit.
     */
    @Test
    public void testGetResId() {
        System.out.println("Testing CKANInputSplit.getResId");
        assertEquals(resId, ckanInputSplit.getResId());
    } // testGetResId
    
    /**
     * Test of getFirstRecordIndex method, of class CKANInputSplit.
     */
    @Test
    public void testGetFirstRecordIndex() {
        System.out.println("Testing CKANInputSplit.getFirstRecordIndex");
        assertEquals(firstRecordIndex, ckanInputSplit.getFirstRecordIndex());
    } // testGetFirstRecordIndex
    
    /**
     * Test of readFields method, of class CKANInputSplit.
     */
    @Test
    public void testReadFields() {
        System.out.println("Testing CKANInputSplit.readFields");
        
        try {
            ckanInputSplit.readFields(dataInput);
            assertTrue(true);
        } catch (Exception e) {
            fail(e.getMessage());
        } // try catch
    } // testReadFields
    
    /**
     * Test of write method, of class CKANInputSplit.
     */
    @Test
    public void testWrite() {
        System.out.println("Testing CKANInputSplit.write");
        
        try {
            ckanInputSplit.write(dataOutput);
            assertTrue(true);
        } catch (Exception e) {
            fail(e.getMessage());
        } // try catch
    } // testWrite
    
} // CKANInputSplitTest
