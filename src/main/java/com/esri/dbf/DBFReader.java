package com.esri.dbf;

import org.apache.hadoop.io.Writable;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Based on https://code.google.com/p/javadbf/
 */
public class DBFReader
{
    private final DataInputStream m_dataInputStream;
    private final DBFHeader m_header;

    public DBFReader(final DataInputStream dataInputStream) throws IOException
    {
        m_dataInputStream = dataInputStream;
        m_header = DBFHeader.read(dataInputStream);
    }

    public Map<String, Object> readRecordAsMap(final Map<String, Object> map) throws IOException
    {
        final byte dataType = nextDataType();
        if (dataType == DBFType.END)
        {
            return null;
        }
        for (final DBFField field : m_header.fields)
        {
            map.put(field.fieldName, field.readValue(m_dataInputStream));
        }
        return map;
    }

    public Map<String, Object> readRecordAsMap() throws IOException
    {
        return readRecordAsMap(new HashMap<String, Object>());
    }

    public Object[] readRecord() throws IOException
    {
        final byte dataType = nextDataType();
        if (dataType == DBFType.END)
        {
            return null;
        }
        final int numberOfFields = m_header.numberOfFields;
        final Object values[] = new Object[numberOfFields];
        for (int i = 0; i < numberOfFields; i++)
        {
            values[i] = readFieldValue(i);
        }
        return values;
    }

    public List<DBFField> getFields()
    {
        return m_header.fields;
    }

    public int getNumberOfFields()
    {
        return m_header.numberOfFields;
    }

    public byte nextDataType() throws IOException
    {
        byte dataType;
        do
        {
            dataType = m_dataInputStream.readByte();
            if (dataType == DBFType.END)
            {
                break;
            }
            else if (dataType == DBFType.DELETED)
            {
                skipRecord();
            }
        }
        while (dataType == DBFType.DELETED);
        return dataType;
    }

    public void skipRecord() throws IOException
    {
        m_dataInputStream.skipBytes(m_header.recordLength - 1);
    }

    public Object readFieldValue(final int index) throws IOException
    {
        return m_header.getField(index).readValue(m_dataInputStream);
    }

    public Writable readFieldWritable(final int index) throws IOException
    {
        return m_header.getField(index).readWritable(m_dataInputStream);
    }

}
