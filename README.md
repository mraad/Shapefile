Shapefile
=========

Simple java library to read point and polygon [shapefiles](http://en.wikipedia.org/wiki/Shapefile).

This library depends on the [Esri Geometry API for Java](https://github.com/Esri/geometry-api-java) under the [GIS Tools For Hadoop](http://esri.github.io/gis-tools-for-hadoop/).

## Sample Shp Usage
```
final File file = new File("cntry06.shp");
final FileInputStream fileInputStream = new FileInputStream(file);
try
{
    final Envelope envelope = new Envelope();
    final Polygon polygon = new Polygon();
    final ShpReader shpReader = new ShpReader(new DataInputStream(new BufferedInputStream(fileInputStream)));
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
```

## Sample DBF Usage
```
final File file = new File("cntry06.dbf");
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
```

## Sample DBF Field Usage
```
final File file = new File("cntry06.dbf");
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
```
