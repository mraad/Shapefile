package com.esri.mapreduce;

import com.esri.io.PolygonWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 */
public class PolygonInputFormat extends AbstractInputFormat<PolygonWritable>
{
    private final class PolygonReader extends AbstractReader<PolygonWritable>
    {
        private final PolygonWritable m_polygonWritable = new PolygonWritable();

        public PolygonReader(
                final InputSplit inputSplit,
                final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
        {
            initialize(inputSplit, taskAttemptContext);
        }

        @Override
        protected void next() throws IOException
        {
            m_shpReader.queryPolygon(m_polygonWritable.polygon);
        }

        @Override
        public PolygonWritable getCurrentValue() throws IOException, InterruptedException
        {
            return m_polygonWritable;
        }

    }

    @Override
    public RecordReader<LongWritable, PolygonWritable> createRecordReader(
            final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        return new PolygonReader(inputSplit, taskAttemptContext);
    }

}
