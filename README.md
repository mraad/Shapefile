Shapefile
=========

Simple java library to read point and polygon [shapefiles](http://en.wikipedia.org/wiki/Shapefile).

In addition, this library contains classes that extend Hadoop `FileInputFormat` and `RecordReader` to enable users to read shapefiles placed in HDFS.
Since shapefiles are relatively "small" to a typical 128MB Hadoop block, the `isSplitable` function implementation always returns false.
That means that only one mapper will be used to read the content of a shapefile in HDFS in an MapReduce job.
The [shapefile specification](http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf) describes an [Index Sequential Access Model](http://en.wikipedia.org/wiki/ISAM) that could be
used to create splits on "large" shapefile. This is on the TODO list :-)
When defining the input path to the job, a user can point to a folder or a set of folders containing both the `.shp` and/or the `.dbf` files.

Input Format              | Writable              | Description
--------------------------|-----------------------|------------
DBFInputFormat            | MapWritable           | Read a DBF file in HDFS
PointInputFormat          | PointWitable          | Read an SHP file in HDFS containing simple 2D point geometry
PolygonInputFormat        | PolygonWitable        | Read an SHP file in HDFS containing simple 2D polygon geometry
PointFeatureInputFormat   | PointFeatureWitable   | Read SHP/DBF files in HDFS containing simple 2D point feature
PolygonFeatureInputFormat | PolygonFeatureWriable | Read SHP/DBF files in HDFS containing simple 2D polygon feature

This library depends on the [Esri Geometry API for Java](https://github.com/Esri/geometry-api-java) under the [GIS Tools For Hadoop](http://esri.github.io/gis-tools-for-hadoop/).

**Clone or pull the latest version of the geometry API and build/install it before building this project.**

## Sample MapReduce Job
```
public class ShapefileTool extends Configured implements Tool
{
    public static void main(final String[] args) throws Exception
    {
        System.exit(ToolRunner.run(new Configuration(), new ShapefileTool(), args));
    }

    @Override
    public int run(final String[] args) throws Exception
    {
        final int rc;
        final JobConf jobConf = new JobConf(getConf(), ShapefileTool.class);

        if (args.length != 2)
        {
            ToolRunner.printGenericCommandUsage(System.err);
            rc = -1;
        }
        else
        {
            jobConf.setJobName(ShapefileTool.class.getSimpleName());

            jobConf.setMapperClass(PolygonFeatureMap.class);

            jobConf.setMapOutputKeyClass(NullWritable.class);
            jobConf.setMapOutputValueClass(Writable.class);

            jobConf.setNumReduceTasks(0);

            jobConf.setInputFormat(PolygonFeatureInputFormat.class);
            jobConf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
            final Path outputDir = new Path(args[1]);
            outputDir.getFileSystem(jobConf).delete(outputDir, true);
            FileOutputFormat.setOutputPath(jobConf, outputDir);

            JobClient.runJob(jobConf);
            rc = 0;
        }
        return rc;
    }
}
```

The mapper function emits the value of the `CNTRY_NAME` attribute and the centroid of the polygon envelope as a `Text` instance:

```
final class PolygonFeatureMap
        extends MapReduceBase
        implements Mapper<LongWritable, PolygonFeatureWritable, NullWritable, Writable>
{
    private final static Text NAME = new Text("CNTRY_NAME");

    private final Text m_text = new Text();
    private final Envelope m_envelope = new Envelope();

    public void map(
            final LongWritable key,
            final PolygonFeatureWritable val,
            final OutputCollector<NullWritable, Writable> collector,
            final Reporter reporter) throws IOException
    {
        val.polygon.queryEnvelope(m_envelope);
        final Point center = m_envelope.getCenter();
        m_text.set(String.format("%.6f %.6f %s",
                center.getX(), center.getY(), val.attributes.get(NAME).toString()));
        collector.collect(NullWritable.get(), m_text);
    }
}
```

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
