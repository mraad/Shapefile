package com.esri.io;

import org.apache.hadoop.io.MapWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 */
public class PolygonFeatureWritable extends PolygonWritable
{
    public final MapWritable attributes = new MapWritable();

    public PolygonFeatureWritable()
    {
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {
        super.write(dataOutput);
        attributes.write(dataOutput);
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {
        super.readFields(dataInput);
        attributes.readFields(dataInput);
    }
}
