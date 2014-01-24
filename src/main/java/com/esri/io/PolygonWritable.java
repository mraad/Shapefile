package com.esri.io;

import com.esri.core.geometry.Point2D;
import com.esri.core.geometry.Polygon;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 */
public class PolygonWritable
        implements Writable
{
    public Polygon polygon;

    public PolygonWritable()
    {
        polygon = new Polygon();
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {
        final int pathCount = polygon.getPathCount();
        dataOutput.writeInt(pathCount);
        for (int i = 0; i < pathCount; i++)
        {
            dataOutput.writeInt(polygon.getPathSize(i));
        }
        final Point2D point2D = new Point2D();
        final int pointCount = polygon.getPointCount();
        for (int i = 0; i < pointCount; i++)
        {
            polygon.getXY(i, point2D); // Had to patch https://github.com/Esri/geometry-api-java
            dataOutput.writeDouble(point2D.x);
            dataOutput.writeDouble(point2D.y);
        }
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {
        polygon.setEmpty();
        final int pathCount = dataInput.readInt();
        final int[] pathSizes = new int[pathCount];
        for (int p = 0; p < pathCount; p++)
        {
            pathSizes[p] = dataInput.readInt();
        }
        for (final int pathSize : pathSizes)
        {
            for (int c = 0; c < pathSize; c++)
            {
                final double x = dataInput.readDouble();
                final double y = dataInput.readDouble();
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
    }
}
