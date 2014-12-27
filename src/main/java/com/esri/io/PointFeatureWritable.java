package com.esri.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 */
public class PointFeatureWritable extends PointWritable
{
    public final Attributes attributes = new Attributes();

    public PointFeatureWritable()
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
