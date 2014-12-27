package com.esri.io;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 */
public class Attributes extends MapWritable
{
    public String getText(final String key)
    {
        final Writable writable = get(new Text(key));
        return writable == null ? "" : writable.toString();
    }

    public long getLong(final String key)
    {
        return getLong(new Text(key));
    }

    public long getLong(
            final Text key
    )
    {
        return getLong(key, 0L);
    }

    public long getLong(
            final Text key,
            final long defaultValue)
    {
        final long rv;
        final Writable writable = get(key);
        if (writable instanceof LongWritable)
        {
            rv = ((LongWritable) writable).get();
        }
        else
        {
            rv = defaultValue;
        }
        return rv;
    }

}
