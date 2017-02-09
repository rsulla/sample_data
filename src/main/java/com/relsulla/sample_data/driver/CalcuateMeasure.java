package com.relsulla.sample_data.driver;

import com.relsulla.sample_data.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.relsulla.sample_data.mapreduce.CalcuateMeasureMapper;
import com.relsulla.sample_data.mapreduce.CalcuateMeasureReducer;

/**
 * Created by Bob on 2/9/2017.
 */
public class CalcuateMeasure extends Configured implements Tool {
    public static void main(String[] args) throws Exception {

        int rc = ToolRunner.run(new CalcuateMeasure(), args);

        System.exit(rc);
    }

    public int run(String[] args) throws Exception {

        int rc = 0;
        Configuration conf;

        conf = getConf();

        return (rc);
    }

    private int runJob(Configuration conf
                      ,String measure
                      ,String inputPaths
                      ,String outPath
                      ,int numReducers) {

        int rc = 0;
        Job job;
        Path outputPath;
        FileSystem fs;

        try {
            job = Job.getInstance(conf, "Sample Data by Measure");

            job.setJarByClass(getClass());
            job.setMapperClass(CalcuateMeasureMapper.class);
            job.setReducerClass(CalcuateMeasureReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);

            outputPath = new Path(outPath);

            fs = Util.getFileSystem(outputPath,conf);

            Util.removeDirIfExists(fs,outPath);

            FileOutputFormat.setOutputPath(job, outputPath);

            if (job.waitForCompletion(true)) {
                rc = 0;
            } else {
                rc = 8;
            }

        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            rc = 8;
        }

        return(rc);
    }
}