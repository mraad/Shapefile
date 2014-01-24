package com.esri.shp;

import java.io.DataInputStream;
import java.io.IOException;

/**
 */
public class ShxReader
{
    private DataInputStream dataInputStream;
    private ShpHeader m_shpHeader;

    public int recordOffset;
    public int recordLength;

    public ShxReader(final DataInputStream dataInputStream) throws IOException
    {
        this.dataInputStream = dataInputStream;
        m_shpHeader = new ShpHeader(dataInputStream);
    }

    public boolean hasMore() throws IOException
    {
        return dataInputStream.available() > 0;
    }

    public int readRecord() throws IOException
    {
        recordOffset = dataInputStream.readInt();
        recordLength = dataInputStream.readInt();
        return recordOffset * 2;
    }

}
