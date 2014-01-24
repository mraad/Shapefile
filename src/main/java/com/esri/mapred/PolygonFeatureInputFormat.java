package com.esri.mapred;

import com.esri.io.PolygonFeatureWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 */
public class PolygonFeatureInputFormat
        extends AbstractInputFormat<PolygonFeatureWritable>
{
    private final class PolygonFeatureReader
            extends AbstractFeatureReader<PolygonFeatureWritable>
    {
        private final PolygonFeatureWritable m_polygonFeatureWritable = new PolygonFeatureWritable();

        public PolygonFeatureReader(
                final InputSplit inputSplit,
                final JobConf jobConf) throws IOException
        {
            super(inputSplit, jobConf);
        }

        @Override
        public PolygonFeatureWritable createValue()
        {
            return m_polygonFeatureWritable;
        }

        @Override
        protected void next() throws IOException
        {
            m_shpReader.queryPolygon(m_polygonFeatureWritable.polygon);
            putAttributes(m_polygonFeatureWritable.attributes);
        }
    }

    @Override
    public RecordReader<LongWritable, PolygonFeatureWritable> getRecordReader(
            final InputSplit inputSplit,
            final JobConf jobConf,
            final Reporter reporter) throws IOException
    {
        return new PolygonFeatureReader(inputSplit, jobConf);
    }

}
