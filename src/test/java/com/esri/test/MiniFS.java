package com.esri.test;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.io.PointWritable;
import com.esri.io.PolygonWritable;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.apache.log4j.Level.ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 */
public class MiniFS
{
    protected FileSystem m_fileSystem;
    protected JobConf m_jobConfig;
    protected MiniDFSCluster m_dfsCluster;

    public void setupMetricsLogging()
    {
        Logger.getLogger(org.apache.hadoop.metrics2.util.MBeans.class).setLevel(ERROR);
        Logger.getLogger(org.apache.hadoop.metrics2.impl.MetricsSystemImpl.class).setLevel(ERROR);
    }

    @Before
    public void setUp() throws Exception
    {
        setupMetricsLogging();

        final File tmpDir = File.createTempFile("dfs", "");

        final Configuration config = new Configuration();
        config.set("hadoop.tmp.dir", tmpDir.getAbsolutePath());

        if (tmpDir.exists())
        {
            FileUtils.forceDelete(tmpDir);
        }
        FileUtils.forceMkdir(tmpDir);

        // used by MiniDFSCluster for DFS storage
        System.setProperty("test.build.data", new File(tmpDir, "data").getAbsolutePath());

        // required by JobHistory.initLogDir
        System.setProperty("hadoop.log.dir", new File(tmpDir, "logs").getAbsolutePath());

        m_jobConfig = new JobConf(config);
        m_dfsCluster = new MiniDFSCluster(m_jobConfig, 1, true, null);
        m_fileSystem = m_dfsCluster.getFileSystem();
    }

    @After
    public void tearDown() throws Exception
    {
        m_dfsCluster.shutdown();
    }

    protected void assertPointValues(final PointWritable value)
    {
        final Point point = value.point;
        assertEquals(-99.79634094297234, point.getX(), 0.000001);
        assertEquals(39.486310278100405, point.getY(), 0.000001);
    }

    protected void assertPolygonValues(final PolygonWritable value)
    {
        final Polygon polygon = value.polygon;
        final Envelope env = new Envelope();
        polygon.queryEnvelope(env);
        assertEquals(-118.45964524998351, env.getXMin(), 0.000001);
        assertEquals(37.038663811607194, env.getYMin(), 0.000001);
        assertEquals(-104.84461178011469, env.getXMax(), 0.000001);
        assertEquals(44.76404797147654, env.getYMax(), 0.000001);
    }

    protected void assertAttributeKeys(final MapWritable value)
    {
        assertTrue(value.containsKey(new Text("AShort")));
        assertTrue(value.containsKey(new Text("ALong")));
        assertTrue(value.containsKey(new Text("AFloat")));
        assertTrue(value.containsKey(new Text("ANume106")));
        assertTrue(value.containsKey(new Text("AText50")));
        assertTrue(value.containsKey(new Text("ADate")));
    }

    protected Path getPath(final String name) throws IOException, URISyntaxException
    {
        final URL resource = getClass().getResource(name);
        final Path path = new Path(name);
        m_fileSystem.copyFromLocalFile(new Path(resource.toURI()), path);
        return path;
    }

    protected void assertPolygonNameValue(final MapWritable attributes)
    {
        assertTrue(attributes.containsKey(new Text("Name")));
        assertEquals(new Text("FooBar"), new Text(attributes.get(new Text("Name")).toString().trim()));
    }
}
