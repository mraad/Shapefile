package com.esri.mapreduce;

import com.esri.io.PointFeatureWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 */
public class PointFeatureReaderTest extends MapreduceFS
{
    @Test
    public void testPointFeatureReader() throws URISyntaxException, IOException, InterruptedException
    {
        final Path dbf = getPath("/testpoint.dbf");
        final Path shp = getPath("/testpoint.shp");
        final FileSplit fileSplit = getFileSplit(shp);
        final PointFeatureInputFormat inputFormat = new PointFeatureInputFormat();
        final TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(m_jobConfig, new TaskAttemptID());
        final RecordReader<LongWritable, PointFeatureWritable> recordReader = inputFormat.createRecordReader(fileSplit, taskAttemptContext);
        assertTrue(recordReader.nextKeyValue());
        final LongWritable key = recordReader.getCurrentKey();
        assertEquals(1L, key.get());
        final PointFeatureWritable value = recordReader.getCurrentValue();
        assertPointValues(value);
        assertAttributeKeys(value.attributes);
        assertFalse(recordReader.nextKeyValue());
        recordReader.close();
    }
}
