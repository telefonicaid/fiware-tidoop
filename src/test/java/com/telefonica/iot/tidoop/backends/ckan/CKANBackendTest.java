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
package com.telefonica.iot.tidoop.backends.ckan;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author frb
 */
@RunWith(MockitoJUnitRunner.class)
public class CKANBackendTest {
    
    // instance to be tested
    private CKANBackend ckanBackend;
    
    // constants
    private final String ckanHost = "localhost";
    private final String ckanPort = "80";
    private final boolean enableSSL = false;
    private final String ckanAPIKey = "ckan_api_key_example";
    private final String orgId = "org_id_example";
    private final int splitsLength = 50;
    
    /**
     * Sets up tests by creating a unique instance of the tested class, and by defining the behaviour of the mocked
     * classes.
     *  
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        // set up the instance of the tested class
        ckanBackend = new CKANBackend(ckanHost, ckanPort, enableSSL, ckanAPIKey, splitsLength);
    } // setUp
    
    /**
     * Test of getPackages method, of class CKANBackend.
     */
    @Test
    public void testGetPackages() {
        System.out.println("Testing CKANBackend.getPackages");
    } // testGetPackages
    
    /**
     * Test of getResources method, of class CKANBackend.
     */
    @Test
    public void testGetResources() {
        System.out.println("Testing CKANBackend.getResources");
    } // testGetResources
    
    /**
     * Test of getNumRecords method, of class CKANBackend.
     */
    @Test
    public void testGetNumRecords() {
        System.out.println("Testing CKANBackend.getNumRecords");
    } // testGetNumRecords
    
    /**
     * Test of getRecords method, of class CKANBackend.
     */
    @Test
    public void testGetRecords() {
        System.out.println("Testing CKANBackend.getRecords");
    } // testGetRecords
    
    /**
     * Test of createResurce method, of class CKANBackend.
     */
    @Test
    public void testCreateResurce() {
        System.out.println("Testing CKANBackend.createResource");
    } // testCreateResurce
    
    /**
     * Test of createKeyValueDatastore method, of class CKANBackend.
     */
    @Test
    public void testCreateKeyValueDatastore() {
        System.out.println("Testing CKANBackend.createKeyValueDatastore");
    } // testCreateKeyValueDatastore
    
    /**
     * Test of insertKeyValue method, of class CKANBackend.
     */
    @Test
    public void testInsertKeyValue() {
        System.out.println("Testing CKANBackend.insertKeyValue");
    } // testInsertKeyValue
    
    /**
     * Test of doCKANRequest method, of class CKANBackend.
     */
    @Test
    public void testDoCKANRequest() {
        System.out.println("Testing CKANBackend.doCKANRequest (no payload)");
        
        System.out.println("Testing CKANBackend.doCKANRequest (with payload)");
    } // testDoCKANRequest
    
} // CKANBackendTest
