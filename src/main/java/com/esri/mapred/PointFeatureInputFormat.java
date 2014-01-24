package com.esri.mapred;

import com.esri.io.PointFeatureWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 */
public class PointFeatureInputFormat
        extends AbstractInputFormat<PointFeatureWritable>
{
    private final class PointFeatureReader
            extends AbstractFeatureReader<PointFeatureWritable>
    {
        private final PointFeatureWritable m_pointFeatureWritable = new PointFeatureWritable();

        public PointFeatureReader(
                final InputSplit inputSplit,
                final JobConf jobConf) throws IOException
        {
            super(inputSplit, jobConf);
        }

        @Override
        public PointFeatureWritable createValue()
        {
            return m_pointFeatureWritable;
        }

        @Override
        protected void next() throws IOException
        {
            m_shpReader.queryPoint(m_pointFeatureWritable.point);
                putAttributes(m_pointFeatureWritable.attributes);
        }

    }

    @Override
    public RecordReader<LongWritable, PointFeatureWritable> getRecordReader(
            final InputSplit inputSplit,
            final JobConf jobConf,
            final Reporter reporter) throws IOException
    {
        return new PointFeatureReader(inputSplit, jobConf);
    }

}
