package com.esri.mapred;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
abstract class AbstractInputFormat<T extends Writable>
        extends FileInputFormat<LongWritable, T>
{
    @Override
    protected FileStatus[] listStatus(final JobConf job) throws IOException
    {
        final FileStatus[] orig = super.listStatus(job);
        final List<FileStatus> list = new ArrayList<FileStatus>(orig.length);
        for (final FileStatus fileStatus : orig)
        {
            final String name = fileStatus.getPath().getName().toLowerCase();
            if (name.endsWith(".shp"))
            {
                list.add(fileStatus);
            }
        }
        final FileStatus[] dest = new FileStatus[list.size()];
        list.toArray(dest);
        return dest;
    }

    @Override
    protected boolean isSplitable(
            final FileSystem fs,
            final Path path)
    {
        return false;
    }

    @Override
    public abstract RecordReader<LongWritable, T> getRecordReader(
            final InputSplit inputSplit,
            final JobConf jobConf,
            final Reporter reporter) throws IOException;
}
