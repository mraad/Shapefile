package com.esri.mapreduce;

import com.esri.io.PolygonFeatureWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 */
public class PolygonFeatureInputFormat extends AbstractInputFormat<PolygonFeatureWritable>
{
    private final class PolygonFeatureReader extends AbstractFeatureReader<PolygonFeatureWritable>
    {
        private final PolygonFeatureWritable m_polygonFeatureWritable = new PolygonFeatureWritable();

        public PolygonFeatureReader(
                final InputSplit inputSplit,
                final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
        {
            initialize(inputSplit, taskAttemptContext);
        }

        @Override
        protected void next() throws IOException
        {
            m_shpReader.queryPolygon(m_polygonFeatureWritable.polygon);
            putAttributes(m_polygonFeatureWritable.attributes);
        }

        @Override
        public PolygonFeatureWritable getCurrentValue() throws IOException, InterruptedException
        {
            return m_polygonFeatureWritable;
        }

    }

    @Override
    public RecordReader<LongWritable, PolygonFeatureWritable> createRecordReader(
            final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        return new PolygonFeatureReader(inputSplit, taskAttemptContext);
    }

}
