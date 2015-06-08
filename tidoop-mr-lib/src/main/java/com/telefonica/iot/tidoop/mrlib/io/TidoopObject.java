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
package com.telefonica.iot.tidoop.mrlib.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * A HDFS writable version of Object.
 * 
 * @author frb
 */
public class TidoopObject implements WritableComparable<TidoopObject> {
    
    private final Logger logger = Logger.getLogger(TidoopObject.class);
    private String data;
    
    /**
     * 
     * @param o
     * @param functionType
     * @return
     */
    public static TidoopObject get(Object o, NumericType functionType) {
        switch (functionType) {
            case INT:
                return new TidoopObject((Integer) o);
            case LONG:
                return new TidoopObject((Long) o);
            case FLOAT:
                return new TidoopObject((Float) o);
            case DOUBLE:
                return new TidoopObject((Double) o);
            case BOOLEAN:
                return new TidoopObject((Boolean) o);
            default:
                return null;
        } // switch
    } // get
    
    /**
     * Constructor.
     * @param i
     */
    public TidoopObject(Integer i) {
        data = i.toString();
    } // TidoopObject
    
    /**
     * Constructor.
     * @param l
     */
    public TidoopObject(Long l) {
        data = l.toString();
    } // TidoopObject
    
    /**
     * Constructor.
     * @param f
     */
    public TidoopObject(Float f) {
        data = f.toString();
    } // TidoopObject
    
    /**
     * Constructor.
     * @param d
     */
    public TidoopObject(Double d) {
        data = d.toString();
    } // TidoopObject
    
    /**
     * Constructor.
     * @param b
     */
    public TidoopObject(Boolean b) {
        data = b.toString();
    } // TidoopObject
    
    /**
     * Constructor.
     * @param s
     */
    public TidoopObject(String s) {
        data = s;
    } // TidoopObject

    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            data = Text.readString(in);
        } catch (Exception e) {
            logger.error("Unable to read TidoopObject fields when deserializing. Details: " + e.getMessage());
        } // try catch
    } // readFields
    
    @Override
    public void write(DataOutput out) throws IOException {
        try {
            Text.writeString(out, data);
        } catch (Exception e) {
            logger.error("Unable to write TidoopObject fields when serializing. Details: " + e.getMessage());
        } // catch
    } // writeFields

    @Override
    public int compareTo(TidoopObject o) {
        throw new UnsupportedOperationException("Not supported yet");
    } // compareTo
    
} // TidoopObject
