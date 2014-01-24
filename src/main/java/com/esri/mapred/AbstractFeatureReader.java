package com.esri.mapred;

import com.esri.dbf.DBFField;
import com.esri.dbf.DBFReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
abstract class AbstractFeatureReader<T extends Writable>
        extends AbstractReader<T>
{
    protected FSDataInputStream m_dbfStream;
    protected DBFReader m_dbfReader;
    protected List<Text> m_keys;

    public AbstractFeatureReader(
            final InputSplit inputSplit,
            final JobConf jobConf) throws IOException
    {
        super(inputSplit, jobConf);
        // No need to check if instance of FileSplit as it is done in super class.
        final FileSplit fileSplit = (FileSplit) inputSplit;
        // Get .shp file
        final Path shpPath = fileSplit.getPath();
        final String dbfName = shpPath.getName().replace(".shp", ".dbf");
        final Path dbfPath = new Path(shpPath.getParent(), dbfName);
        m_dbfStream = dbfPath.getFileSystem(jobConf).open(dbfPath);
        m_dbfReader = new DBFReader(m_dbfStream);
        // Create a list of field name as Hadoop Text instances
        final List<DBFField> fields = m_dbfReader.getFields();
        m_keys = new ArrayList<Text>(fields.size());
        for (final DBFField field : fields)
        {
            m_keys.add(new Text(field.fieldName));
        }
    }

    protected void putAttributes(final MapWritable attributes) throws IOException
    {
        final int len = m_dbfReader.getNumberOfFields();
        m_dbfReader.nextDataType();
        for (int i = 0; i < len; i++)
        {
            attributes.put(m_keys.get(i), m_dbfReader.readFieldWritable(i));
        }
    }

    @Override
    public void close() throws IOException
    {
        // Close shp
        super.close();
        if (m_dbfStream != null)
        {
            m_dbfStream.close();
            m_dbfStream = null;
        }
    }

}
