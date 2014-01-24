package com.esri.mapred;

import com.esri.io.PointFeatureWritable;
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
public class PointFeatureReaderTest extends MapredFS
{
    @Test
    public void testPointFeatureReader() throws IOException, URISyntaxException
    {
        final Path dbf = getPath("/testpoint.dbf");

        final Path shp = getPath("/testpoint.shp");

        final FileSplit fileSplit = getFileSplit(shp);

        final PointFeatureInputFormat inputFormat = new PointFeatureInputFormat();
        final RecordReader<LongWritable, PointFeatureWritable> recordReader = inputFormat.getRecordReader(fileSplit, m_jobConfig, null);
        final LongWritable key = recordReader.createKey();
        final PointFeatureWritable value = recordReader.createValue();
        assertTrue(recordReader.next(key, value));
        assertPointValues(value);
        assertAttributeKeys(value.attributes);
        assertFalse(recordReader.next(key, value));
        recordReader.close();
    }
}
