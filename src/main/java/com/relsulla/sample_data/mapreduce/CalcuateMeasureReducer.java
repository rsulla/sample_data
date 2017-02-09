package com.relsulla.sample_data.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Bob on 2/9/2017.
 */
public class CalcuateMeasureReducer extends Reducer<Text, Text, NullWritable, Text> {
}
