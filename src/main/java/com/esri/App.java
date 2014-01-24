package com.esri;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.dbf.DBFReader;
import com.esri.dbf.DBFType;
import com.esri.shp.ShpReader;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 */
public class App
{
    public static void main(String[] args) throws IOException
    {
        readDBFRaw();
    }

    private static void readDBFRaw() throws IOException
    {
        final File file = new File("/Users/mraad_admin/Downloads/cntry06/cntry06.dbf");
        final FileInputStream fileInputStream = new FileInputStream(file);
        try
        {
            final DBFReader dbfReader = new DBFReader(new DataInputStream(new BufferedInputStream(fileInputStream)));

            System.out.println(dbfReader.getFields());

            final int numberOfFields = dbfReader.getNumberOfFields();
            byte dataType = dbfReader.nextDataType();
            while (dataType != DBFType.END)
            {
                System.out.println("-----");
                for (int i = 0; i < numberOfFields; i++)
                {
                    System.out.println(dbfReader.readFieldWritable(i));
                }
                dataType = dbfReader.nextDataType();
            }
        }
        finally
        {
            fileInputStream.close();
        }
    }

    private static void readDBF() throws IOException
    {
        final File file = new File("/Users/mraad_admin/Downloads/points1.dbf");
        final FileInputStream fileInputStream = new FileInputStream(file);
        try
        {
            final Map<String, Object> map = new HashMap<String, Object>();
            final DBFReader dbfReader = new DBFReader(new DataInputStream(new BufferedInputStream(fileInputStream)));
            while (dbfReader.readRecordAsMap(map) != null)
            {
                System.out.println(map);
            }
        }
        finally
        {
            fileInputStream.close();
        }
    }

    private static void readShp() throws IOException
    {
        final File file = new File("/Users/mraad_admin/Downloads/cntry06/cntry06.shp");
        final FileInputStream fileInputStream = new FileInputStream(file);
        try
        {
            final Envelope envelope = new Envelope();
            final Polygon polygon = new Polygon();
            final ShpReader shpReader = new ShpReader(new DataInputStream(new BufferedInputStream(fileInputStream, 10 * 1024)));
            while (shpReader.hasMore())
            {
                shpReader.queryPolygon(polygon);
                polygon.queryEnvelope(envelope);
                final Point center = envelope.getCenter();
                System.out.format("%.6f %.6f%n", center.getX(), center.getY());
            }
        }
        finally
        {
            fileInputStream.close();
        }
    }
}
