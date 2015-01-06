package com.esri.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 */
public class PolylineMWritable implements Writable, Serializable
{
    public int[] lens; // The length in each part - lens.length is the number of parts
    public double[] x;
    public double[] y;
    public double[] m;

    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {
        dataOutput.writeInt(lens.length);
        for (final int len : lens)
        {
            dataOutput.writeInt(len);
        }
        final int length = x.length;
        dataOutput.writeInt(length);
        for (int l = 0; l < length; l++)
        {
            dataOutput.writeDouble(x[l]);
            dataOutput.writeDouble(y[l]);
            dataOutput.writeDouble(m[l]);
        }
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {
        final int plen = dataInput.readInt();
        lens = new int[plen];
        for (int l = 0; l < plen; l++)
        {
            lens[l] = dataInput.readInt();
        }
        final int clen = dataInput.readInt();
        x = new double[clen];
        y = new double[clen];
        m = new double[clen];
        for (int l = 0; l < clen; l++)
        {
            x[l] = dataInput.readDouble();
            y[l] = dataInput.readDouble();
            m[l] = dataInput.readDouble();
        }
    }
}
