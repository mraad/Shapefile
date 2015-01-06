package com.esri.shp;

import org.apache.commons.io.EndianUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 */
public class ShpHeader implements Serializable
{
    public transient int fileLength;
    public transient int version;
    public transient int shapeType;
    public transient double xmin;
    public transient double ymin;
    public transient double xmax;
    public transient double ymax;
    public transient double zmin;
    public transient double zmax;
    public transient double mmin;
    public transient double mmax;

    public ShpHeader(final DataInputStream dataInputStream) throws IOException
    {
        final int signature = dataInputStream.readInt();
        if (signature != 9994)
        {
            throw new IOException("Not a valid shapefile. Expected 9994 as file header !");
        }

        dataInputStream.skip(5 * 4);

        fileLength = dataInputStream.readInt();

        version = EndianUtils.readSwappedInteger(dataInputStream);
        shapeType = EndianUtils.readSwappedInteger(dataInputStream);

        xmin = EndianUtils.readSwappedDouble(dataInputStream);
        ymin = EndianUtils.readSwappedDouble(dataInputStream);
        xmax = EndianUtils.readSwappedDouble(dataInputStream);
        ymax = EndianUtils.readSwappedDouble(dataInputStream);
        zmin = EndianUtils.readSwappedDouble(dataInputStream);
        zmax = EndianUtils.readSwappedDouble(dataInputStream);
        mmin = EndianUtils.readSwappedDouble(dataInputStream);
        mmax = EndianUtils.readSwappedDouble(dataInputStream);
    }

}
