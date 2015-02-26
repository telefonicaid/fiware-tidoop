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
package com.telefonica.iot.tidoop.backends.ckan;

import org.json.simple.JSONObject;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author frb
 */
@RunWith(MockitoJUnitRunner.class)
public class CKANResponseTest {
    
    // instance to be tested
    private CKANResponse ckanResponse;
    
    /**
     * Sets up tests by creating a unique instance of the tested class, and by defining the behaviour of the mocked
     * classes.
     *  
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        // set up the instance of the tested class
        ckanResponse = new CKANResponse(new JSONObject(), 200);
    } // setUp
    
    /**
     * Test of getJsonObject method, of class CKANResponse.
     */
    @Test
    public void testGetJsonObject() {
        System.out.println("Testing CKANResponse.getJsonObject");
        JSONObject jsonObject = ckanResponse.getJsonObject();
        assertTrue(jsonObject != null);
    } // testGetJsonObject
    
    /**
     * Test of getStatusCode method, of class CKANResponse.
     */
    @Test
    public void testGetStatusCode() {
        System.out.println("Testing CKANResponse.getStatusCode");
        int statusCode = ckanResponse.getStatusCode();
        assertEquals(200, statusCode);
    } // testGetStatusCode
    
} // CKANResponseTest
