package com.esri.mapred;

import com.esri.io.PointWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 */
public class PointReaderTest extends MapredFS
{
    @Test
    public void testPointReader() throws IOException, URISyntaxException
    {
        final Path shp = getPath("/testpoint.shp");
        final FileSplit fileSplit = getFileSplit(shp);
        final PointInputFormat inputFormat = new PointInputFormat();
        final RecordReader<LongWritable, PointWritable> recordReader = inputFormat.getRecordReader(fileSplit, m_jobConfig, null);
        final LongWritable key = recordReader.createKey();
        final PointWritable value = recordReader.createValue();
        assertTrue(recordReader.next(key, value));
        assertPointValues(value);
        assertFalse(recordReader.next(key, value));
        recordReader.close();
    }

}
