package com.esri.mapreduce;

import com.esri.dbf.DBFField;
import com.esri.dbf.DBFReader;
import com.esri.dbf.DBFType;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class DBFRecordReader
        extends RecordReader<LongWritable, MapWritable>
{
    protected final LongWritable m_recordNumber = new LongWritable();
    protected final MapWritable m_mapWritable = new MapWritable();
    protected long m_length;
    protected FSDataInputStream m_dbfStream;
    protected DBFReader m_dbfReader;
    protected long m_recno;
    private ArrayList<Text> m_keys;

    public DBFRecordReader(
            final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public void initialize(
            final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        if (inputSplit instanceof FileSplit)
        {
            final FileSplit fileSplit = (FileSplit) inputSplit;
            m_length = fileSplit.getLength();
            final Path path = fileSplit.getPath();
            final FileSystem fileSystem = path.getFileSystem(taskAttemptContext.getConfiguration());
            m_dbfStream = fileSystem.open(path);
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
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
        return m_recordNumber;
    }

    @Override
    public MapWritable getCurrentValue() throws IOException, InterruptedException
    {
        return m_mapWritable;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        final boolean hasNext = m_dbfReader.nextDataType() != DBFType.END;
        if (hasNext)
        {
            m_recordNumber.set(m_recno++);
            final int numFields = m_dbfReader.getNumberOfFields();
            for (int i = 0; i < numFields; i++)
            {
                m_mapWritable.put(m_keys.get(i), m_dbfReader.readFieldWritable(i));
            }
        }
        return hasNext;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        return m_dbfStream.getPos() / m_length;
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
