package com.esri.dbf;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.GregorianCalendar;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 */
public class DBFTest
{
    @Test
    public void testDBFWritable() throws IOException
    {
        final InputStream inputStream = this.getClass().getResourceAsStream("/testpoint.dbf");
        assertNotNull(inputStream);
        try
        {
            final DBFReader dbfReader = new DBFReader(new DataInputStream(inputStream));
            final List<DBFField> fields = dbfReader.getFields();
            assertEquals(6, fields.size());
            assertField(fields.get(0), "AShort", 'N', 4, 0);
            assertField(fields.get(1), "ALong", 'N', 9, 0);
            assertField(fields.get(2), "AFloat", 'F', 13, 11);
            assertField(fields.get(3), "ANume106", 'N', 11, 6);
            assertField(fields.get(4), "AText50", 'C', 50, 0);
            assertField(fields.get(5), "ADate", 'D', 8, 0);

            assertEquals(1, dbfReader.getNumberOfRecords());

            assertNotEquals(DBFType.END, dbfReader.nextDataType());

            Writable writable = dbfReader.readFieldWritable(0);
            assertTrue(writable instanceof IntWritable);
            assertEquals(123, ((IntWritable) writable).get());

            writable = dbfReader.readFieldWritable(1);
            assertTrue(writable instanceof LongWritable);
            assertEquals(12345, ((LongWritable) writable).get());

            writable = dbfReader.readFieldWritable(2);
            assertTrue(writable instanceof FloatWritable);
            assertEquals(123.4, ((FloatWritable) writable).get(), 0.001);

            writable = dbfReader.readFieldWritable(3);
            assertTrue(writable instanceof DoubleWritable);
            assertEquals(123.45678, ((DoubleWritable) writable).get(), 0.000001);

            writable = dbfReader.readFieldWritable(4);
            assertTrue(writable instanceof Text);
            assertEquals("Hello, World", writable.toString().trim());

            writable = dbfReader.readFieldWritable(5);
            assertTrue(writable instanceof LongWritable);
            final GregorianCalendar gregorianCalendar = new GregorianCalendar(2014, 5 - 1, 20);
            assertEquals(gregorianCalendar.getTimeInMillis(), ((LongWritable) writable).get());
        }
        finally
        {
            inputStream.close();
        }
    }

    @Test
    public void testDBFValues() throws IOException
    {
        final InputStream inputStream = this.getClass().getResourceAsStream("/testpoint.dbf");
        assertNotNull(inputStream);
        try
        {
            final DataInputStream dataInputStream = new DataInputStream(inputStream);
            final DBFReader dbfReader = new DBFReader(dataInputStream);

            assertNotEquals(DBFType.END, dbfReader.nextDataType());

            Object value = dbfReader.readFieldValue(0);
            assertTrue(value instanceof Short);
            assertEquals((short) 123, value);

            value = dbfReader.readFieldValue(1);
            assertTrue(value instanceof Long);
            assertEquals(12345L, value);

            value = dbfReader.readFieldValue(2);
            assertTrue(value instanceof Float);
            assertEquals(123.4F, (Float) value, 0.001);

            value = dbfReader.readFieldValue(3);
            assertTrue(value instanceof Double);
            assertEquals(123.45678, (Double) value, 0.000001);

            value = dbfReader.readFieldValue(4);
            assertTrue(value instanceof String);
            assertEquals("Hello, World", value.toString().trim());

            value = dbfReader.readFieldValue(5);
            assertTrue(value instanceof Long);
            final GregorianCalendar gregorianCalendar = new GregorianCalendar(2014, 5 - 1, 20);
            assertEquals(gregorianCalendar.getTimeInMillis(), value);

            assertEquals(DBFType.END, dbfReader.nextDataType());
        }
        finally
        {
            inputStream.close();
        }
    }

    private void assertField(
            final DBFField dbfField,
            final String fieldName,
            final char dataType,
            final int fieldLength,
            final int decimalCount)
    {
        assertEquals(fieldName, dbfField.fieldName);
        assertEquals(dataType, dbfField.dataType);
        assertEquals(fieldLength, dbfField.fieldLength);
        assertEquals(decimalCount, dbfField.decimalCount);
    }
}
