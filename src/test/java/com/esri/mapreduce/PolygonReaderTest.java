package com.esri.mapreduce;

import com.esri.io.PolygonWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 */
public class PolygonReaderTest extends MapreduceFS
{
    @Test
    public void testPolygonReader() throws IOException, URISyntaxException, InterruptedException
    {
        final Path shp = getPath("/testpolygon.shp");
        final FileSplit fileSplit = getFileSplit(shp);
        final PolygonInputFormat pointInputFormat = new PolygonInputFormat();
        final TaskAttemptContext taskAttemptContext = new TaskAttemptContext(m_jobConfig, new TaskAttemptID());
        final RecordReader<LongWritable, PolygonWritable> recordReader = pointInputFormat.createRecordReader(fileSplit, taskAttemptContext);
        assertTrue(recordReader.nextKeyValue());
        assertEquals(1L, recordReader.getCurrentKey().get());
        assertPolygonValues(recordReader.getCurrentValue());
        assertFalse(recordReader.nextKeyValue());
        recordReader.close();
    }
}
