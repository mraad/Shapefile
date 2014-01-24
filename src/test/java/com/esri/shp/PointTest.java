package com.esri.shp;

import com.esri.core.geometry.Point;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 */
public class PointTest
{
    @Test
    public void testReadPoint() throws IOException
    {
        final InputStream inputStream = this.getClass().getResourceAsStream("/testpoint.shp");
        assertNotNull(inputStream);
        try
        {
            // One point shapefile - so extent is the point
            final ShpReader shpReader = new ShpReader(new DataInputStream(inputStream));
            final ShpHeader shpHeader = shpReader.getHeader();
            assertTrue(shpReader.hasMore());
            final Point point = shpReader.readPoint();
            assertEquals(shpHeader.xmin, point.getX(), 0.000001);
            assertEquals(shpHeader.ymin, point.getY(), 0.000001);
        }
        finally
        {
            inputStream.close();
        }
    }

    @Test
    public void testQueryPoint() throws IOException
    {
        final InputStream inputStream = this.getClass().getResourceAsStream("/testpoint.shp");
        assertNotNull(inputStream);
        try
        {
            // One point shapefile - so extent is the point
            final ShpReader shpReader = new ShpReader(new DataInputStream(inputStream));
            final ShpHeader shpHeader = shpReader.getHeader();
            assertEquals(1, shpHeader.shapeType);
            assertTrue(shpReader.hasMore());
            final Point point = new Point();
            shpReader.queryPoint(point);
            assertEquals(shpHeader.xmin, point.getX(), 0.000001);
            assertEquals(shpHeader.ymin, point.getY(), 0.000001);
        }
        finally
        {
            inputStream.close();
        }
    }
}
