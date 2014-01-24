package com.esri.mapreduce;

import com.esri.shp.ShpReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 */
abstract class AbstractReader<T extends Writable>
        extends RecordReader<LongWritable, T>
{
    protected final LongWritable m_recordNumber = new LongWritable();
    protected long m_length;
    protected FSDataInputStream m_shpStream;
    protected ShpReader m_shpReader;

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
            m_shpStream = fileSystem.open(path);
            m_shpReader = new ShpReader(m_shpStream);
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
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        final boolean hasMore = m_shpReader.hasMore();
        if (hasMore)
        {
            next();
            m_recordNumber.set(m_shpReader.recordNumber);
        }
        return hasMore;
    }

    protected abstract void next() throws IOException;

    @Override
    public abstract T getCurrentValue() throws IOException, InterruptedException;

    @Override
    public float getProgress() throws IOException, InterruptedException
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
}
