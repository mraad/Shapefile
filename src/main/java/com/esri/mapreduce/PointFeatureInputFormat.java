package com.esri.mapreduce;

import com.esri.io.PointFeatureWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 */
public class PointFeatureInputFormat extends AbstractInputFormat<PointFeatureWritable>
{
    private final class PointFeatureReader extends AbstractFeatureReader<PointFeatureWritable>
    {
        private final PointFeatureWritable m_pointFeatureWritable = new PointFeatureWritable();

        public PointFeatureReader(
                final InputSplit inputSplit,
                final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
        {
            initialize(inputSplit, taskAttemptContext);
        }

        @Override
        protected void next() throws IOException
        {
            m_shpReader.queryPoint(m_pointFeatureWritable.point);
            putAttributes(m_pointFeatureWritable.attributes);
        }

        @Override
        public PointFeatureWritable getCurrentValue() throws IOException, InterruptedException
        {
            return m_pointFeatureWritable;
        }

    }

    @Override
    public RecordReader<LongWritable, PointFeatureWritable> createRecordReader(
            final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        return new PointFeatureReader(inputSplit, taskAttemptContext);
    }

}
