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
package com.telefonica.iot.tidoop.mrlib;

import com.telefonica.iot.tidoop.mrlib.MapOnly.CustomMapper;
import com.telefonica.iot.tidoop.mrlib.utils.Constants;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
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
public class MapOnlyTest {
    
    // instances to be tested
    private CustomMapper mapper;
    
    // mocks
    @Mock
    private Mapper.Context mockContextMapper;
    @Mock
    private Configuration mockConfiguration;
    
    // constants
    private final String functionStr = "long y = x * x";
    private final Text line = new Text("6");
    
    /**
     * Sets up tests by creating a unique instance of the tested class, and by defining the behaviour of the mocked
     * classes.
     *  
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        // set up the instances of the tested classes
        mapper = new CustomMapper();
        
        // set up the behaviour of the mocked classes
        when(mockContextMapper.getConfiguration()).thenReturn(mockConfiguration);
        when(mockConfiguration.get(Constants.PARAM_FUNCTION, "long y = x")).thenReturn(functionStr);
    } // setUp
    
    /**
     * Test of setup method, of class CustomMapper.
     */
    @Test
    public void testLineFilterSetup() {
        try {
            mapper.setup(mockContextMapper);
        } catch (IOException e) {
            fail(e.getMessage());
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } finally {
            assertTrue(true);
        } // try catch finally
    } // testLineFilterSetup
    
    /**
     * Test of map method, of class CustomMapper. The function is applied to the line.
     */
    @Test
    public void testLineFilterMapRegexMatching() {
        try {
            mapper.setup(mockContextMapper);
            mapper.map(null, line, mockContextMapper);
        } catch (IOException e) {
            fail(e.getMessage());
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } finally {
            assertTrue(true);
        } // try catch finally
    } // testLineFilterMapRegexMatching
    
} // MapOnlyTest
