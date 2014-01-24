package com.esri.mapreduce;

import com.esri.test.MiniFS;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 */
public class MapreduceFS extends MiniFS
{
    protected FileSplit getFileSplit(final Path dst) throws IOException
    {
        final long len = m_fileSystem.getFileStatus(dst).getLen();
        return new FileSplit(dst, 0, len, null);
    }
}
