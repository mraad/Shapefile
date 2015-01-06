package com.esri.shp;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 */
public class ShxReader implements Serializable
{
    private transient DataInputStream dataInputStream;

    public transient ShpHeader shpHeader;
    public transient long recordOffset;
    public transient int recordLength;

    public ShxReader(final DataInputStream dataInputStream) throws IOException
    {
        this.dataInputStream = dataInputStream;
        shpHeader = new ShpHeader(dataInputStream);
    }

    public boolean hasMore() throws IOException
    {
        return dataInputStream.available() > 0;
    }

    /**
     * Read an SHX record.
     *
     * @return the seek position into the SHP file.
     * @throws IOException if an IO error occurs.
     */
    public long readRecord() throws IOException
    {
        recordOffset = dataInputStream.readInt();
        recordLength = dataInputStream.readInt();
        return recordOffset << 1;
    }

}
