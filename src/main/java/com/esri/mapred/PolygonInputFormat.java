package com.esri.mapred;

import com.esri.io.PolygonWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 */
public class PolygonInputFormat
        extends AbstractInputFormat<PolygonWritable>
{
    private final class PolygonReader
            extends AbstractReader<PolygonWritable>
    {
        private final PolygonWritable m_polygonWritable = new PolygonWritable();

        public PolygonReader(
                final InputSplit inputSplit,
                final JobConf jobConf) throws IOException
        {
            super(inputSplit, jobConf);
        }

        @Override
        public PolygonWritable createValue()
        {
            return m_polygonWritable;
        }

        @Override
        protected void next() throws IOException
        {
            m_shpReader.queryPolygon(m_polygonWritable.polygon);
        }
    }

    @Override
    public RecordReader<LongWritable, PolygonWritable> getRecordReader(
            final InputSplit inputSplit,
            final JobConf jobConf,
            final Reporter reporter) throws IOException
    {
        return new PolygonReader(inputSplit, jobConf);
    }

}
