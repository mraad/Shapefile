package com.esri.shp;

import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.io.PolylineMWritable;
import org.apache.commons.io.EndianUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf
 */
public class ShpReader implements Serializable
{
    private transient DataInputStream m_dataInputStream;
    private transient ShpHeader m_shpHeader;

    private transient int m_parts[] = new int[4];

    public transient int recordNumber;
    public transient int contentLength;
    public transient int contentLengthInBytes;
    public transient int shapeType;
    public transient double xmin;
    public transient double ymin;
    public transient double xmax;
    public transient double ymax;
    public transient double mmin;
    public transient double mmax;
    public transient int numParts;
    public transient int numPoints;

    public ShpReader(final DataInputStream dataInputStream) throws IOException
    {
        m_dataInputStream = dataInputStream;
        m_shpHeader = new ShpHeader(dataInputStream);
    }

    public ShpHeader getHeader()
    {
        return m_shpHeader;
    }

    public boolean hasMore() throws IOException
    {
        return m_dataInputStream.available() > 0;
    }

    private void readRecordHeader() throws IOException
    {
        recordNumber = m_dataInputStream.readInt();
        contentLength = m_dataInputStream.readInt();
        contentLengthInBytes = contentLength + contentLength - 4;

        shapeType = EndianUtils.readSwappedInteger(m_dataInputStream);
    }

    public Point readPoint() throws IOException
    {
        return queryPoint(new Point());
    }

    public Polygon readPolygon() throws IOException
    {
        return queryPolygon(new Polygon());
    }

    public Point queryPoint(final Point point) throws IOException
    {
        readRecordHeader();
        point.setX(EndianUtils.readSwappedDouble(m_dataInputStream));
        point.setY(EndianUtils.readSwappedDouble(m_dataInputStream));
        return point;
    }

    public Polygon queryPolygon(final Polygon polygon) throws IOException
    {
        polygon.setEmpty();

        readRecordHeader();

        readShapeHeader();

        for (int i = 0, j = 1; i < numParts; )
        {
            final int count = m_parts[j++] - m_parts[i++];
            for (int c = 0; c < count; c++)
            {
                final double x = EndianUtils.readSwappedDouble(m_dataInputStream);
                final double y = EndianUtils.readSwappedDouble(m_dataInputStream);
                if (c > 0)
                {
                    polygon.lineTo(x, y);
                }
                else
                {
                    polygon.startPath(x, y);
                }
            }
        }

        polygon.closeAllPaths();

        return polygon;
    }

    public PolylineMWritable readPolylineMWritable() throws IOException
    {
        final PolylineMWritable polylineMWritable = new PolylineMWritable();

        readRecordHeader();
        readShapeHeader();

        polylineMWritable.lens = new int[numParts];
        polylineMWritable.x = new double[numPoints];
        polylineMWritable.y = new double[numPoints];
        polylineMWritable.m = new double[numPoints];

        int p = 0;
        for (int i = 0, j = 1; i < numParts; i++, j++)
        {
            final int count = m_parts[j] - m_parts[i];
            polylineMWritable.lens[i] = count;
            for (int c = 0; c < count; c++, p++)
            {
                polylineMWritable.x[p] = EndianUtils.readSwappedDouble(m_dataInputStream);
                polylineMWritable.y[p] = EndianUtils.readSwappedDouble(m_dataInputStream);
            }
        }

        mmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        mmax = EndianUtils.readSwappedDouble(m_dataInputStream);

        for (p = 0; p < numPoints; p++)
        {
            polylineMWritable.m[p] = EndianUtils.readSwappedDouble(m_dataInputStream);
        }

        return polylineMWritable;
    }

    private void readShapeHeader() throws IOException
    {
        xmin = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymin = EndianUtils.readSwappedDouble(m_dataInputStream);
        xmax = EndianUtils.readSwappedDouble(m_dataInputStream);
        ymax = EndianUtils.readSwappedDouble(m_dataInputStream);

        numParts = EndianUtils.readSwappedInteger(m_dataInputStream);
        numPoints = EndianUtils.readSwappedInteger(m_dataInputStream);

        if ((numParts + 1) > m_parts.length)
        {
            m_parts = new int[numParts + 1];
        }
        for (int p = 0; p < numParts; p++)
        {
            m_parts[p] = EndianUtils.readSwappedInteger(m_dataInputStream);
        }
        m_parts[numParts] = numPoints;
    }

}
