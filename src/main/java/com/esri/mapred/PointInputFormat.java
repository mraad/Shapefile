package com.esri.mapred;

import com.esri.io.PointWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 */
public class PointInputFormat
        extends AbstractInputFormat<PointWritable>
{
    private final class PointReader
            extends AbstractReader<PointWritable>
    {
        private final PointWritable m_pointWritable = new PointWritable();

        public PointReader(
                final InputSplit inputSplit,
                final JobConf jobConf) throws IOException
        {
            super(inputSplit, jobConf);
        }

        @Override
        public PointWritable createValue()
        {
            return m_pointWritable;
        }

        @Override
        protected void next() throws IOException
        {
            m_shpReader.queryPoint(m_pointWritable.point);
        }
    }

    @Override
    public RecordReader<LongWritable, PointWritable> getRecordReader(
            final InputSplit inputSplit,
            final JobConf jobConf,
            final Reporter reporter) throws IOException
    {
        return new PointReader(inputSplit, jobConf);
    }
}
