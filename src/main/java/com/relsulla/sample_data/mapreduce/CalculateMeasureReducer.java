package com.relsulla.sample_data.mapreduce;

import com.relsulla.sample_data.util.Util;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

import com.relsulla.sample_data.mapreduce.CalculateMeasureMapper;

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

    private StringBuffer outValueText = new StringBuffer();

    private Text outValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {

        measureCount = 0;
        highLaceCount = 0;

        valueIterator = values.iterator();

        while ( valueIterator.hasNext() ) {
            parts = valueIterator.next().toString().split(Util.FIELD_SEPARATOR);

            measureCount++;

            if ( parts[CalculateMeasureMapper.OUT_COL_HIGH_LACE_SCORE].equals("Y") ) {
                highLaceCount++;
            }
        }

        if ( measureCount > 0 ) {
            highLaceScorePercent = (double) highLaceCount / (double) measureCount;

            outValue.clear();

            outValueText.setLength(0);
            outValueText.append(String.valueOf(highLaceCount));
            outValueText.append(Util.FIELD_SEPARATOR);
            outValueText.append(String.valueOf(measureCount));
            outValueText.append(Util.FIELD_SEPARATOR);
            outValueText.append(String.valueOf(highLaceScorePercent));

            outValue.set(outValueText.toString());

            context.write(key,outValue);
        }
    }
}
