package com.esri.mapred;

import com.esri.dbf.DBFField;
import com.esri.dbf.DBFReader;
import com.esri.dbf.DBFType;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class DBFRecordReader
        implements RecordReader<LongWritable, MapWritable>
{
    protected final LongWritable m_key = new LongWritable();
    protected final MapWritable m_value = new MapWritable();
    protected float m_length;
    protected FSDataInputStream m_dbfStream;
    protected DBFReader m_dbfReader;
    protected List<Text> m_keys;
    protected long m_recno;

    public DBFRecordReader(
            final InputSplit inputSplit,
            final JobConf jobConf) throws IOException
    {
        if (inputSplit instanceof FileSplit)
        {
            final FileSplit fileSplit = (FileSplit) inputSplit;
            m_length = fileSplit.getLength();
            final Path path = fileSplit.getPath();
            m_dbfStream = path.getFileSystem(jobConf).open(path);
            m_dbfReader = new DBFReader(m_dbfStream);

            final List<DBFField> fields = m_dbfReader.getFields();
            m_keys = new ArrayList<Text>(fields.size());
            for (final DBFField field : fields)
            {
                m_keys.add(new Text(field.fieldName));
            }
        }
        else
        {
            throw new IOException("Input split is not an instance of FileSplit");
        }
    }

    @Override
    public LongWritable createKey()
    {
        return m_key;
    }

    @Override
    public MapWritable createValue()
    {
        return m_value;
    }

    @Override
    public long getPos() throws IOException
    {
        return m_dbfStream.getPos();
    }

    @Override
    public float getProgress() throws IOException
    {
        return m_dbfStream.getPos() / m_length;
    }

    @Override
    public boolean next(
            final LongWritable key,
            final MapWritable value) throws IOException
    {
        final boolean hasNext = m_dbfReader.nextDataType() != DBFType.END;
        if (hasNext)
        {
            m_key.set(m_recno++);
            final int numFields = m_dbfReader.getNumberOfFields();
            for (int i = 0; i < numFields; i++)
            {
                value.put(m_keys.get(i), m_dbfReader.readFieldWritable(i));
            }
        }
        return hasNext;
    }

    @Override
    public void close() throws IOException
    {
        if (m_dbfStream != null)
        {
            m_dbfStream.close();
            m_dbfStream = null;
        }
    }

}
