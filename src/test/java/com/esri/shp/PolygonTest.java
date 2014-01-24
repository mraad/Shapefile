package com.esri.shp;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Polygon;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 */
public class PolygonTest
{
    @Test
    public void testReadPolygon() throws IOException
    {
        final InputStream inputStream = this.getClass().getResourceAsStream("/testpolygon.shp");
        assertNotNull(inputStream);
        try
        {
            final ShpReader shpReader = new ShpReader(new DataInputStream(inputStream));
            final ShpHeader shpHeader = shpReader.getHeader();
            assertTrue(shpReader.hasMore());
            final Polygon polygon = shpReader.readPolygon();
            final Envelope2D enveloper2D = new Envelope2D();
            polygon.queryEnvelope2D(enveloper2D);
            assertEquals(shpHeader.xmin, enveloper2D.xmin, 0.000001);
            assertEquals(shpHeader.ymin, enveloper2D.ymin, 0.000001);
            assertEquals(shpHeader.xmax, enveloper2D.xmax, 0.000001);
            assertEquals(shpHeader.ymax, enveloper2D.ymax, 0.000001);
        }
        finally
        {
            inputStream.close();
        }
    }

    @Test
    public void testQueryPolygon() throws IOException
    {
        final InputStream inputStream = this.getClass().getResourceAsStream("/testpolygon.shp");
        assertNotNull(inputStream);
        try
        {
            final ShpReader shpReader = new ShpReader(new DataInputStream(inputStream));
            final ShpHeader shpHeader = shpReader.getHeader();
            assertEquals(5, shpHeader.shapeType);
            assertTrue(shpReader.hasMore());
            final Polygon polygon = new Polygon();
            shpReader.queryPolygon(polygon);
            final Envelope2D enveloper2D = new Envelope2D();
            polygon.queryEnvelope2D(enveloper2D);
            assertEquals(shpHeader.xmin, enveloper2D.xmin, 0.000001);
            assertEquals(shpHeader.ymin, enveloper2D.ymin, 0.000001);
            assertEquals(shpHeader.xmax, enveloper2D.xmax, 0.000001);
            assertEquals(shpHeader.ymax, enveloper2D.ymax, 0.000001);
        }
        finally
        {
            inputStream.close();
        }
    }
}
