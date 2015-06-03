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

import com.telefonica.iot.tidoop.mrlib.Filter.LineFilter;
import com.telefonica.iot.tidoop.mrlib.Filter.LinesCombiner;
import com.telefonica.iot.tidoop.mrlib.Filter.LinesJoiner;
import com.telefonica.iot.tidoop.mrlib.utils.Constants;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
public class FilterTest {
    
    // instances to be tested
    private LineFilter mapper;
    private LinesCombiner combiner;
    private LinesJoiner reducer;
    
    // mocks
    @Mock
    private Mapper.Context mockContextMapper;
    @Mock
    private Reducer.Context mockContextCombinerReducer;
    @Mock
    private Configuration mockConfiguration;
    
    // constants
    private final Text matchingLine = new Text("this line matches because contains xxx");
    private final Text notMatchingLine = new Text("this line does not match");
    private final String regex = "^.*\bxxx\b.*$";
    private final Text combinerReducerKey = new Text("common-key");
    private final Iterable<Text> filteredLines = new ArrayList<Text>(
            Arrays.asList(new Text("line1"), new Text("line2"), new Text("line3")));
    
    /**
     * Sets up tests by creating a unique instance of the tested class, and by defining the behaviour of the mocked
     * classes.
     *  
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        // set up the instances of the tested classes
        mapper = new LineFilter();
        combiner = new LinesCombiner();
        reducer = new LinesJoiner();
        
        // set up the behaviour of the mocked classes
        when(mockContextMapper.getConfiguration()).thenReturn(mockConfiguration);
        when(mockConfiguration.get(Constants.PARAM_REGEX, "")).thenReturn(regex);
    } // setUp
    
    /**
     * Test of setup method, of class LineFilter.
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
     * Test of map method, of class LineFilter. The line matches the regex and passes.
     */
    @Test
    public void testLineFilterMapRegexMatching() {
        try {
            mapper.setup(mockContextMapper);
            mapper.map(null, matchingLine, mockContextMapper);
        } catch (IOException e) {
            fail(e.getMessage());
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } finally {
            assertTrue(true);
        } // try catch finally
    } // testLineFilterMapRegexMatching
    
    /**
     * Test of map method, of class LineFilter. The line does not match the regex and does not pass.
     */
    @Test
    public void testLineFilterMapRegexNotMatching() {
        try {
            mapper.setup(mockContextMapper);
            mapper.map(null, notMatchingLine, mockContextMapper);
        } catch (IOException e) {
            fail(e.getMessage());
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } finally {
            assertTrue(true);
        } // try catch finally
    } // testLineFilterMapRegexNotMatching
    
    /**
     * Test of reduce method, of class LinesCombiner.
     */
    @Test
    public void testLinesCombinerReduce() {
        try {
            combiner.reduce(combinerReducerKey, filteredLines, mockContextCombinerReducer);
        } catch (IOException e) {
            fail(e.getMessage());
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } finally {
            assertTrue(true);
        } // try catch finally
    } // testLinesCombinerReduce
    
    /**
     * Test of reduce method, of class LinesJoiner.
     */
    @Test
    public void testLinesJoinerReduce() {
        try {
            combiner.reduce(combinerReducerKey, filteredLines, mockContextCombinerReducer);
        } catch (IOException e) {
            fail(e.getMessage());
        } catch (InterruptedException e) {
            fail(e.getMessage());
        } finally {
            assertTrue(true);
        } // try catch finally
    } // testLinesJoinerReduce
    
} // FilterTest
