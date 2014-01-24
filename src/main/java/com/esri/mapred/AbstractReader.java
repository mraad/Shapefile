package com.esri.mapred;

import com.esri.shp.ShpReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 */
abstract class AbstractReader<T extends Writable>
        implements RecordReader<LongWritable, T>
{
    protected final LongWritable m_key = new LongWritable();
    protected float m_length;
    protected FSDataInputStream m_shpStream;
    protected ShpReader m_shpReader;

    public AbstractReader(
            final InputSplit inputSplit,
            final JobConf jobConf) throws IOException
    {
        if (inputSplit instanceof FileSplit)
        {
            final FileSplit fileSplit = (FileSplit) inputSplit;
            m_length = fileSplit.getLength();
            final Path path = fileSplit.getPath();
            m_shpStream = path.getFileSystem(jobConf).open(path);
            m_shpReader = new ShpReader(m_shpStream);
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
    public long getPos() throws IOException
    {
        return m_shpStream.getPos();
    }

    @Override
    public float getProgress() throws IOException
    {
        return m_shpStream.getPos() / m_length;
    }

    @Override
    public void close() throws IOException
    {
        if (m_shpStream != null)
        {
            m_shpStream.close();
            m_shpStream = null;
        }
    }

    @Override
    public boolean next(
            final LongWritable key,
            final T value) throws IOException
    {
        final boolean hasMore = m_shpReader.hasMore();
        if (hasMore)
        {
            next();
            m_key.set(m_shpReader.recordNumber);
        }
        return hasMore;
    }

    protected abstract void next() throws IOException;

    @Override
    public abstract T createValue();

}
