package com.esri.io;

import com.esri.core.geometry.Point;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 */
public class PointWritable implements Writable
{
    public Point point;

    public PointWritable()
    {
        point = new Point();
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {
        dataOutput.writeDouble(point.getX());
        dataOutput.writeDouble(point.getY());
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {
        point.setEmpty();
        point.setX(dataInput.readDouble());
        point.setY(dataInput.readDouble());
    }
}
