package com.esri.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 */
public class DBFReaderTest extends MapreduceFS
{
    @Test
    public void testDBFReader() throws URISyntaxException, IOException, InterruptedException
    {
        final Path dbf = getPath("/testpoint.dbf");
        final FileSplit fileSplit = getFileSplit(dbf);
        final TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(m_jobConfig, new TaskAttemptID());
        final DBFRecordReader dbfRecordReader = new DBFRecordReader(fileSplit, taskAttemptContext);
        assertTrue(dbfRecordReader.nextKeyValue());
        assertEquals(0L, dbfRecordReader.getCurrentKey().get());
        assertAttributeKeys(dbfRecordReader.getCurrentValue());
        dbfRecordReader.close();
    }

}
