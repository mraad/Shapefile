package com.esri.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 */
public class DBFReaderTest extends MapredFS
{
    @Test
    public void testDBFReader() throws URISyntaxException, IOException
    {
        final Path dbf = getPath("/testpoint.dbf");
        final FileSplit fileSplit = getFileSplit(dbf);
        final DBFRecordReader dbfRecordReader = new DBFRecordReader(fileSplit, m_jobConfig);
        final LongWritable key = dbfRecordReader.createKey();
        final MapWritable value = dbfRecordReader.createValue();
        assertTrue(dbfRecordReader.next(key, value));
        assertEquals(0L, key.get());
        assertAttributeKeys(value);
        dbfRecordReader.close();
    }

}
