package com.esri.io;

import com.esri.core.geometry.Point2D;
import com.esri.core.geometry.Polygon;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 */
public class PolygonWritableTest
{
    @Test
    public void testWriteRead() throws IOException
    {
        final ByteArrayOutputStream byteArrayOutputStream = getByteArrayOutputStream();

        final DataInput dataInput = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        final PolygonWritable polygonWritable = new PolygonWritable();
        polygonWritable.readFields(dataInput);

        final Polygon polygon = polygonWritable.polygon;
        final Point2D[] coordinates2D = polygon.getCoordinates2D();

        assertEquals(9, coordinates2D.length);

        assertEquals(0.0, coordinates2D[0].x, 0.000001);
        assertEquals(0.0, coordinates2D[0].y, 0.000001);

        assertEquals(10.0, coordinates2D[1].x, 0.000001);
        assertEquals(0.0, coordinates2D[1].y, 0.000001);

        assertEquals(10.0, coordinates2D[2].x, 0.000001);
        assertEquals(10.0, coordinates2D[2].y, 0.000001);

        assertEquals(0.0, coordinates2D[3].x, 0.000001);
        assertEquals(10.0, coordinates2D[3].y, 0.000001);

        assertEquals(0.0, coordinates2D[4].x, 0.000001);
        assertEquals(0.0, coordinates2D[4].y, 0.000001);

        assertEquals(20.0, coordinates2D[5].x, 0.000001);
        assertEquals(20.0, coordinates2D[5].y, 0.000001);

        assertEquals(30.0, coordinates2D[6].x, 0.000001);
        assertEquals(20.0, coordinates2D[6].y, 0.000001);

        assertEquals(30.0, coordinates2D[7].x, 0.000001);
        assertEquals(30.0, coordinates2D[7].y, 0.000001);

        assertEquals(20.0, coordinates2D[8].x, 0.000001);
        assertEquals(20.0, coordinates2D[8].y, 0.000001);

    }

    private ByteArrayOutputStream getByteArrayOutputStream() throws IOException
    {
        final PolygonWritable polygonWritable = new PolygonWritable();
        final Polygon polygon = polygonWritable.polygon;

        polygon.startPath(0, 0);
        polygon.lineTo(10, 0);
        polygon.lineTo(10, 10);
        polygon.lineTo(0, 10);
        polygon.lineTo(0, 0);

        polygon.startPath(20, 20);
        polygon.lineTo(30, 20);
        polygon.lineTo(30, 30);
        polygon.lineTo(20, 20);

        polygon.closeAllPaths();

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final DataOutput dataOutput = new DataOutputStream(byteArrayOutputStream);
        polygonWritable.write(dataOutput);

        byteArrayOutputStream.flush();
        return byteArrayOutputStream;
    }
}
