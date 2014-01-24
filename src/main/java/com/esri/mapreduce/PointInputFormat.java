package com.esri.mapreduce;

import com.esri.io.PointWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 */
public class PointInputFormat extends AbstractInputFormat<PointWritable>
{
    private final class PointReader extends AbstractReader<PointWritable>
    {
        private final PointWritable m_pointWritable = new PointWritable();

        public PointReader(
                final InputSplit inputSplit,
                final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
        {
            initialize(inputSplit, taskAttemptContext);
        }

        @Override
        protected void next() throws IOException
        {
            m_shpReader.queryPoint(m_pointWritable.point);
        }

        @Override
        public PointWritable getCurrentValue() throws IOException, InterruptedException
        {
            return m_pointWritable;
        }

    }

    @Override
    public RecordReader<LongWritable, PointWritable> createRecordReader(
            final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        return new PointReader(inputSplit, taskAttemptContext);
    }

}
