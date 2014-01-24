package com.esri.io;

import com.esri.core.geometry.Point2D;
import com.esri.core.geometry.Polygon;
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
public class PolygonFeatureWritableTest
{
    @Test
    public void testWriteRead() throws Exception
    {
        final ByteArrayOutputStream byteArrayOutputStream = getByteArrayOutputStream();

        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        final PolygonFeatureWritable polygonFeatureWritable = new PolygonFeatureWritable();
        polygonFeatureWritable.readFields(new DataInputStream(byteArrayInputStream));

        final Polygon polygon = polygonFeatureWritable.polygon;
        final Point2D[] coordinates2D = polygon.getCoordinates2D();
        assertEquals(4, coordinates2D.length);

        assertEquals(0, coordinates2D[0].x, 0.000001);
        assertEquals(0, coordinates2D[0].y, 0.000001);

        assertEquals(10, coordinates2D[1].x, 0.000001);
        assertEquals(0, coordinates2D[1].y, 0.000001);

        assertEquals(10, coordinates2D[2].x, 0.000001);
        assertEquals(10, coordinates2D[2].y, 0.000001);

        assertEquals(0, coordinates2D[3].x, 0.000001);
        assertEquals(0, coordinates2D[3].y, 0.000001);

        assertEquals(new LongWritable(1234), polygonFeatureWritable.attributes.get(new Text("key")));
    }

    private ByteArrayOutputStream getByteArrayOutputStream() throws IOException
    {
        final PolygonFeatureWritable pointFeatureWritable = new PolygonFeatureWritable();
        final Polygon polygon = pointFeatureWritable.polygon;
        polygon.startPath(0, 0);
        polygon.lineTo(10, 0);
        polygon.lineTo(10, 10);
        polygon.lineTo(0, 0);
        polygon.closeAllPaths();

        pointFeatureWritable.attributes.put(new Text("key"), new LongWritable(1234));

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        pointFeatureWritable.write(new DataOutputStream(byteArrayOutputStream));
        byteArrayOutputStream.flush();
        return byteArrayOutputStream;
    }
}
