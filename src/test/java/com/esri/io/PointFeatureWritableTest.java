package com.esri.io;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 */
public class PointFeatureWritableTest
{
    @Test
    public void testWriteRead() throws Exception
    {
        final ByteArrayOutputStream byteArrayOutputStream = getByteArrayOutputStream();

        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        final PointFeatureWritable pointFeatureWritable = new PointFeatureWritable();
        pointFeatureWritable.readFields(new DataInputStream(byteArrayInputStream));

        assertEquals(10.0, pointFeatureWritable.point.getX(), 0.000001);
        assertEquals(11.0, pointFeatureWritable.point.getY(), 0.000001);
        assertEquals(new LongWritable(1234), pointFeatureWritable.attributes.get(new Text("key")));
    }

    private ByteArrayOutputStream getByteArrayOutputStream() throws IOException
    {
        final PointFeatureWritable pointFeatureWritable = new PointFeatureWritable();
        pointFeatureWritable.point.setXY(10, 11);
        pointFeatureWritable.attributes.put(new Text("key"), new LongWritable(1234));

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        pointFeatureWritable.write(new DataOutputStream(byteArrayOutputStream));
        byteArrayOutputStream.flush();
        return byteArrayOutputStream;
    }

}
