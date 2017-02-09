package com.relsulla.sample_data.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Bob on 2/9/2017.
 */
public class CalcuateMeasureMapper extends Mapper<LongWritable, Text, Text, Text> {
}
