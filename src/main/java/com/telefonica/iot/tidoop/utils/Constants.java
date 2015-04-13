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

package com.telefonica.iot.tidoop.utils;

/**
 *
 * @author frb
 */
public final class Constants {
    
    /**
     * Constructor. It is private since utility classes should not have a public or default constructor.
     */
    private Constants() {
    } // Constants

    // HTTP constants
    public static final int MAX_CONNS = 500;
    public static final int MAX_CONNS_PER_ROUTE = 100;

    // MapReduce constants
    public static final String INPUT_CKAN_HOST = "mapreduce.input.ckaninputformat.host";
    public static final String INPUT_CKAN_PORT = "mapreduce.input.ckaninputformat.port";
    public static final String INPUT_CKAN_SSL = "mapreduce.input.ckaninputformat.ssl";
    public static final String INPUT_CKAN_API_KEY = "mapreduce.input.ckaninputformat.apikey";
    public static final String INPUT_CKAN_URLS = "mapreduce.input.ckaninputformat.inputurls";
    public static final String INPUT_CKAN_SPLITS_LENGTH = "mapreduce.input.ckaninputformat.splitslength";
    public static final String OUTPUT_CKAN_HOST = "mapreduce.output.ckanoutputformat.host";
    public static final String OUTPUT_CKAN_PORT = "mapreduce.output.ckanoutputformat.port";
    public static final String OUTPUT_CKAN_SSL = "mapreduce.output.ckanoutputformat.ssl";
    public static final String OUTPUT_CKAN_API_KEY = "mapreduce.output.ckanoutputformat.apikey";
    public static final String OUTPUT_CKAN_URL = "mapreduce.output.ckanoutputformat.outputurl";

} // Constants
