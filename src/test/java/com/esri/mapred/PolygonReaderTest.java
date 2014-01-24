package com.esri.mapred;

import com.esri.io.PolygonWritable;
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
public class PolygonReaderTest extends MapredFS
{
    @Test
    public void testPolygonReader() throws IOException, URISyntaxException
    {
        final Path shp = getPath("/testpolygon.shp");
        final FileSplit fileSplit = getFileSplit(shp);
        final PolygonInputFormat inputFormat = new PolygonInputFormat();
        final RecordReader<LongWritable, PolygonWritable> recordReader = inputFormat.getRecordReader(fileSplit, m_jobConfig, null);
        final LongWritable key = recordReader.createKey();
        final PolygonWritable value = recordReader.createValue();
        assertTrue(recordReader.next(key, value));
        assertPolygonValues(value);
        assertFalse(recordReader.next(key, value));
        recordReader.close();
    }
}
