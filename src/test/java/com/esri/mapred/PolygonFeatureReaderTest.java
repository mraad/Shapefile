package com.esri.mapred;

import com.esri.io.PolygonFeatureWritable;
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
public class PolygonFeatureReaderTest extends MapredFS
{
    @Test
    public void testPolygonFeatureReader() throws IOException, URISyntaxException
    {
        final Path dbf = getPath("/testpolygon.dbf");
        final Path shp = getPath("/testpolygon.shp");
        final FileSplit fileSplit = getFileSplit(shp);
        final PolygonFeatureInputFormat inputFormat = new PolygonFeatureInputFormat();
        final RecordReader<LongWritable, PolygonFeatureWritable> recordReader = inputFormat.getRecordReader(fileSplit, m_jobConfig, null);
        final LongWritable key = recordReader.createKey();
        final PolygonFeatureWritable value = recordReader.createValue();
        assertTrue(recordReader.next(key, value));
        assertPolygonValues(value);
        assertPolygonNameValue(value.attributes);
        assertFalse(recordReader.next(key, value));
        recordReader.close();
    }
}
