package com.relsulla.sample_data.mapreduce;

import com.relsulla.sample_data.util.Util;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Bob on 2/9/2017.
 */
public class CalculateMeasureReducer extends Reducer<Text, Text, Text, Text> {

    private int measureCount = 0;
    private int highLaceCount = 0;
    private int laceScore = 0;
    private double highLaceScorePercent;
    private String[] parts;
    private Iterator<Text> valueIterator;

    private Text outValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {

        measureCount = 0;
        highLaceCount = 0;

        valueIterator = values.iterator();

        while ( valueIterator.hasNext() ) {
            parts = valueIterator.next().toString().split(Util.FIELD_SEPARATOR);

            measureCount++;

            laceScore = Integer.parseInt(parts[2]);

            if ( laceScore > 9 ) {
                highLaceCount++;
            }
        }

        if ( measureCount > 0 ) {
            highLaceScorePercent = (double) highLaceCount / (double) measureCount;

            outValue.clear();
            outValue.set(String.valueOf(highLaceScorePercent));

            context.write(key,outValue);
        }
    }
}
