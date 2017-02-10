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

import java.net.URI;

/**
 * Created by Bob on 2/9/2017.
 */
public class CalcuateMeasure extends Configured implements Tool {

    public static final String DIAGNOSIS_CODES_CACHE     = "CalculateMeasuer.diagnosisCodesTable";
    public static final String COMORBIDITY_COLUMNS_CACHE = "CalculateMeasuer.comorbidityColumnsTable";
    public static final String MEASURE_COMORBIDITY_CACHE = "CalculateMeasuer.measureComorbidity";

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
                      ,String diagnosisCodesTablePath
                      ,String comorbidityColumnsTablePath
                      ,String measureComorbidityPath
                      ,String outPath
                      ,int numReducers) {

        int rc = 0;
        Job job;
        Path outputPath;
        FileSystem fs;
        Path diagnosisCodesTableCache;
        Path comorbidityColumnsTableCache;
        Path measureComorbidityCache;

        try {
            job = Job.getInstance(conf, "Sample Data by Measure (" + measure + ")");

            job.setJarByClass(getClass());
            job.setMapperClass(CalcuateMeasureMapper.class);
            job.setReducerClass(CalcuateMeasureReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(TextInputFormat.class);

            diagnosisCodesTableCache = new Path(diagnosisCodesTablePath);
            conf.set(DIAGNOSIS_CODES_CACHE,diagnosisCodesTableCache.getName());
            job.addCacheFile(new URI(diagnosisCodesTableCache.toString() + "#" + diagnosisCodesTableCache.getName()));

            comorbidityColumnsTableCache = new Path(comorbidityColumnsTablePath);
            conf.set(COMORBIDITY_COLUMNS_CACHE,comorbidityColumnsTableCache.getName());
            job.addCacheFile(new URI(comorbidityColumnsTableCache.toString() + "#" + comorbidityColumnsTableCache.getName()));

            measureComorbidityCache = new Path(measureComorbidityPath);
            conf.set(MEASURE_COMORBIDITY_CACHE,measureComorbidityCache.getName());
            job.addCacheFile(new URI(measureComorbidityCache.toString() + "#" + measureComorbidityCache.getName()));

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