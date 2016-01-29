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

import com.telefonica.iot.tidoop.apiext.backends.ckan.CKANBackend;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;

/**
 * Custom RecordReader for CKAN data.
 * 
 * @author frb
 */
public class CKANRecordReader extends RecordReader<LongWritable, Text> { // FIXME: Json or CKANRecord instead of Text?
    
    private final Logger logger;
    private final CKANBackend backend;
    private final InputSplit split;
    private final TaskAttemptContext context;
    private long start;
    private long length;
    private int current; // FIXME: this should be a long integer... but arrays do not accept such a large index
    private JSONArray records;
    private LongWritable key;
    private Text value;
    
    /**
     * Constructor.
     * @param backend
     * @param split
     * @param context
     */
    public CKANRecordReader(CKANBackend backend, InputSplit split, TaskAttemptContext context) {
        this.logger = Logger.getLogger(CKANRecordReader.class);
        this.backend = backend;
        this.split = split;
        this.context = context;
    } // CKANRecordReader
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        CKANInputSplit ckanInputSplit = (CKANInputSplit) this.split;
        start = ckanInputSplit.getFirstRecordIndex();
        length = ckanInputSplit.getLength();
        current = 0;
        
        // query CKAN for the related resource, seeking to the start of the split
        records = backend.getRecords(ckanInputSplit.getResId(), start, start + length);
    } // initialize
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (current < length) {
            key = new LongWritable(current);
            value = new Text(records.get(current).toString());
            current++;
            return true;
        } else {
            return false;
        } // if else
    } // nextKeyValue
    
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    } // getCurrentKey
    
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    } // getCurrentValue
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return Math.min(1.0f, current / (float) (length));
    } // getProgress
    
    @Override
    public void close() throws IOException {
        // nothig to close, using the CKAN REST API there is no resource "opening" at all
    } // close
    
} // CKANRecordReader
