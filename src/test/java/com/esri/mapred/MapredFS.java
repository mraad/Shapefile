package com.esri.mapred;

import com.esri.test.MiniFS;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;

/**
 */
public class MapredFS extends MiniFS
{
    protected FileSplit getFileSplit(final Path path) throws IOException
    {
        final long len = m_fileSystem.getFileStatus(path).getLen();
        return new FileSplit(path, 0, len, (String[]) null);
    }
}
