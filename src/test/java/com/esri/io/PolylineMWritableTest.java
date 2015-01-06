package com.esri.io;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 */
public class PolylineMWritableTest
{
    @Test
    public void testWriteRead() throws IOException
    {
        final ByteArrayOutputStream byteArrayOutputStream = getByteArrayOutputStream();

        final DataInput dataInput = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        final PolylineMWritable polylineM = new PolylineMWritable();
        polylineM.readFields(dataInput);

        assertNotNull(polylineM.lens);
        assertEquals(1, polylineM.lens.length);
        assertEquals(2, polylineM.lens[0]);

        assertNotNull(polylineM.x);
        assertNotNull(polylineM.y);
        assertNotNull(polylineM.m);

        assertEquals(2, polylineM.x.length);
        assertEquals(2, polylineM.y.length);
        assertEquals(2, polylineM.m.length);

        assertEquals(0.0, polylineM.x[0], 0.000001);
        assertEquals(100.0, polylineM.x[1], 0.000001);

        assertEquals(0.0, polylineM.y[0], 0.000001);
        assertEquals(200.0, polylineM.y[1], 0.000001);

        assertEquals(10.0, polylineM.m[0], 0.000001);
        assertEquals(20.0, polylineM.m[1], 0.000001);

    }

    private ByteArrayOutputStream getByteArrayOutputStream() throws IOException
    {
        final PolylineMWritable polylineM = new PolylineMWritable();

        polylineM.lens = new int[]{2};
        polylineM.x = new double[]{0, 100};
        polylineM.y = new double[]{0, 200};
        polylineM.m = new double[]{10, 20};

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final DataOutput dataOutput = new DataOutputStream(byteArrayOutputStream);
        polylineM.write(dataOutput);

        byteArrayOutputStream.flush();
        return byteArrayOutputStream;
    }
}
