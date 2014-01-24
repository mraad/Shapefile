package com.esri.mapreduce;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.List;

/**
 */
public class DBFInputFormat
        extends FileInputFormat<LongWritable, MapWritable>
{
    @Override
    protected List<FileStatus> listStatus(final JobContext job) throws IOException
    {
        final List<FileStatus> list = super.listStatus(job);
        for (final FileStatus fileStatus : list)
        {
            if (!fileStatus.getPath().getName().toLowerCase().endsWith(".dbf"))
            {
                list.remove(fileStatus);
            }
        }
        return list;
    }

    @Override
    protected boolean isSplitable(
            final JobContext context,
            final Path filename)
    {
        return false;
    }

    @Override
    public RecordReader<LongWritable, MapWritable> createRecordReader(
            final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        return new DBFRecordReader(inputSplit, taskAttemptContext);
    }
}
