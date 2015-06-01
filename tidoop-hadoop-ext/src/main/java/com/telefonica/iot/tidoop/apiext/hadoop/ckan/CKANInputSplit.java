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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;

/**
 * Custom InputSplit for CKAN data.
 * 
 * @author frb
 */
public class CKANInputSplit extends InputSplit implements Writable {
    
    private Logger logger;
    private String resId;
    private long firstRecordIndex;
    private long length;
    
    /**
     * Constructor.
     */
    public CKANInputSplit() {
    } // CKANInputSplit
    
    /**
     * Constructor.
     * @param resId
     * @param firstRecordIndex
     * @param length
     */
    public CKANInputSplit(String resId, long firstRecordIndex, long length) {
        this.logger = Logger.getLogger(CKANInputSplit.class);
        logger.info("Creating split (resId=" + resId + ", first record index=" + firstRecordIndex
                + ", split length=" + length + ")");
        this.resId = resId;
        this.firstRecordIndex = firstRecordIndex;
        this.length = length;
    } // CKANInputSplit
    
    @Override
    public long getLength() throws IOException, InterruptedException {
        return length;
    } // getLength
    
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[] {};
    } // getLocations
    
    public String getResId() {
        return resId;
    } // getResId
    
    public long getFirstRecordIndex() {
        return firstRecordIndex;
    } // getFirstRecordIndex
    
    @Override
    public void readFields(DataInput in) {
        try {
            resId = Text.readString(in);
            firstRecordIndex = in.readLong();
            length = in.readLong();
        } catch (Exception e) {
            logger.error("Unable to read CKANInputSplit fields when deserializing. Details: " + e.getMessage());
        } // try catch
    } // readFields
    
    @Override
    public void write(DataOutput out) {
        try {
            Text.writeString(out, resId);
            out.writeLong(firstRecordIndex);
            out.writeLong(length);
        } catch (Exception e) {
            logger.error("Unable to write CKANInputSplit fields when serializing. Details: " + e.getMessage());
        } // catch
    } // write

} // CKANInputSplit
