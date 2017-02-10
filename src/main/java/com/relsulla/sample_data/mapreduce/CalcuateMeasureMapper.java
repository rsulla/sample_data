package com.relsulla.sample_data.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.HashMap;

import com.relsulla.sample_data.driver.CalcuateMeasure;

/**
 * Created by Bob on 2/9/2017.
 */
public class CalcuateMeasureMapper extends Mapper<LongWritable, Text, Text, Text> {

    private HashMap<String, Integer> comorbidityColumnsMap = new HashMap<String, Integer>();
    private HashMap<String, String> diagnosisCodesMap = new HashMap<String, String>();
    private HashMap<String, String> measureComorbidityMap = new HashMap<String, String>();

    public void setup(Context context) {

        URI[] cacheFiles;
        Configuration conf;

        String comorbidityColumnsSetting = "";
        String diagnosisCodesSettingSetting = "";
        String measureComorbiditySetting = "";

        String[] parts;

        try {
            conf = context.getConfiguration();

            comorbidityColumnsSetting = conf.get(CalcuateMeasure.COMORBIDITY_COLUMNS_CACHE);
            diagnosisCodesSettingSetting = conf.get(CalcuateMeasure.DIAGNOSIS_CODES_CACHE);
            measureComorbiditySetting = conf.get(CalcuateMeasure.MEASURE_COMORBIDITY_CACHE);

            cacheFiles = context.getCacheFiles();

            for (int idxPath = 0; idxPath < cacheFiles.length; idxPath++) {
                parts = cacheFiles[idxPath].toString().split("#");

                if (parts[1].equals(comorbidityColumnsSetting)) {
                    getComorbidityColumnsMap(parts[1]);

                } else if (parts[1].equals(diagnosisCodesSettingSetting)) {
                    getDiagnosisCodesMap(parts[1]);

                } else if (parts[1].equals(measureComorbiditySetting)) {
                    getMeasureComorbidityMap(parts[1]);

                }
            }

        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(8);
        }
    }

    private void getComorbidityColumnsMap(String comorbidityColumnsFile) {

        BufferedReader br;
        String line;
        int idx = 0;

        try {
            br = new BufferedReader(new FileReader("./" + comorbidityColumnsFile));

            while ((line = br.readLine()) != null) {
                comorbidityColumnsMap.put(line, idx);
                idx++;
            }

            br.close();
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(8);
        }

    }

    private void getDiagnosisCodesMap(String diagnosisCodesFile) {

        BufferedReader br;
        String line;
        String[] lineParts;
        String[] parts;

        try {
            br = new BufferedReader(new FileReader("./" + diagnosisCodesFile));

            while ((line = br.readLine()) != null) {
                lineParts = line.split("\\t");
                parts = lineParts[1].split(",");

                for (int idx = 0; idx < parts.length; idx++) {
                    measureComorbidityMap.put(parts[idx],lineParts[0]);
                }
            }

            br.close();
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(8);
        }

    }

    private void getMeasureComorbidityMap(String measureComorbidityFile) {

        BufferedReader br;
        String line;
        String[] lineParts;
        String[] parts;

        try {
            br = new BufferedReader(new FileReader("./" + measureComorbidityFile));

            while ((line = br.readLine()) != null) {
                lineParts = line.split("\\t");
                parts = lineParts[1].split(",");

                for (int idx = 0; idx < parts.length; idx++) {
                    diagnosisCodesMap.put(lineParts[0], parts[idx]);
                }
            }

            br.close();
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(8);
        }
    }
}
