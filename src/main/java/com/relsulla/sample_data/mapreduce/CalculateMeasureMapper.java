package com.relsulla.sample_data.mapreduce;

import com.relsulla.sample_data.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.relsulla.sample_data.driver.CalculateMeasure;

/**
 * Created by Bob on 2/9/2017.
 */
public class CalculateMeasureMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final int COL_ENCOUNTER_ID      = 0;
    private static final int COL_PATIENT_NBR       = 1;
    private static final int COL_RACE              = 2;
    private static final int COL_GENDER            = 3;
    private static final int COL_AGE               = 4;
    private static final int COL_LENGTH_OF_STAY    = 5;
    private static final int COL_ED_VISITS         = 6;
    private static final int COL_INPATIENT_VISITS  = 7;
    private static final int COL_DIAGNOSIS_CODE    = 8;
    private static final int COL_FIRST_COMORBIDITY = 9;

    public static final int HIGH_LACE_SCORE_VALUE          = 10;

    public static final int OUT_COL_ENCOUNTER_ID           = 0;
    public static final int OUT_COL_DIAGNOSIS_CODE         = 1;
    public static final int OUT_COL_LENGTH_OF_STAY         = 2;
    public static final int OUT_COL_LENGTH_OF_STAY_SCORE   = 3;
    public static final int OUT_COL_ACUTE_ADMISSIONS       = 4;
    public static final int OUT_COL_ACUTE_ADMISSIONS_SCORE = 5;
    public static final int OUT_COL_COMORBIDITY_SUB_SCORE  = 6;
    public static final int OUT_COL_COMORBIDITY_SCORE      = 7;
    public static final int OUT_COL_ED_VISITS              = 8;
    public static final int OUT_COL_ED_VISITS_SCORE        = 9;
    public static final int OUT_COL_LACE_SCORE             = 10;
    public static final int OUT_COL_HIGH_LACE_SCORE        = 11;

    private HashMap<String, Integer> comorbidityColumnsMap = new HashMap<String, Integer>();
    private HashMap<String,String> diagnosisCodesMap = new HashMap<String, String>();
    private HashMap<String, HashMap<String,String>> measureComorbidityMap = new HashMap<String, HashMap<String,String>>();

    private String[] inputValues;
    private String selectedMeasure;
    private String measure;
    private int maxCol = 99999;
    private int col;

    private int lengthOfStay;
    private int acuteAdmissions;
    private int comorbiditySubScore;
    private int edVists;

    private int lengthOfStayScore;
    private int acuteAdmissionsScore;
    private int comorbidityScore;
    private int edVistsScore;
    private int laceScore;

    private char laceScoreFl;

    private StringBuffer outValueText = new StringBuffer();
    private Text outKey = new Text();
    private Text outValue = new Text();

    public void setup(Context context) {

        URI[] cacheFiles;
        Configuration conf;

        String comorbidityColumnsSetting = "";
        String diagnosisCodesSetting = "";
        String measureComorbiditySetting = "";

        String[] parts;

        try {
            conf = context.getConfiguration();

            selectedMeasure = conf.get(CalculateMeasure.SELECTED_MEASURE);

            comorbidityColumnsSetting = conf.get(CalculateMeasure.COMORBIDITY_COLUMNS_CACHE);
            diagnosisCodesSetting = conf.get(CalculateMeasure.DIAGNOSIS_CODES_CACHE);
            measureComorbiditySetting = conf.get(CalculateMeasure.MEASURE_COMORBIDITY_CACHE);

            cacheFiles = context.getCacheFiles();

            for (int idxPath = 0; idxPath < cacheFiles.length; idxPath++) {
                parts = cacheFiles[idxPath].toString().split("#");

                if (parts[1].equals(comorbidityColumnsSetting)) {
                    getComorbidityColumnsMap(parts[1],context);

                } else if (parts[1].equals(diagnosisCodesSetting)) {
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

    private void getComorbidityColumnsMap(String comorbidityColumnsFile,Context context) throws Exception {

        BufferedReader br;
        String line;
        int idx = 0;

        try {
            br = new BufferedReader(new FileReader("./" + comorbidityColumnsFile));

            while ((line = br.readLine()) != null) {
                comorbidityColumnsMap.put(line.trim(), idx);
                maxCol = idx;
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
                    diagnosisCodesMap.put(parts[idx].trim(), lineParts[0].trim());
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

                HashMap<String,String> comorbidityMap = new HashMap<String,String>();

                for (int idx = 0; idx < parts.length; idx++) {
                    comorbidityMap.put(parts[idx].trim(), "");
                }

                measureComorbidityMap.put(lineParts[0].trim(), comorbidityMap);
            }

            br.close();
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(8);
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        comorbiditySubScore = 0;
        inputValues = value.toString().split(",");

        if ( inputValues.length >= (COL_FIRST_COMORBIDITY + maxCol) ) {
            if ( diagnosisCodesMap.containsKey(inputValues[COL_DIAGNOSIS_CODE]) ) {
                measure = diagnosisCodesMap.get(inputValues[COL_DIAGNOSIS_CODE]);

                if ( (measure.equals(selectedMeasure) || selectedMeasure.equals("ALL")) && measureComorbidityMap.containsKey(measure) ) {
                    for (Map.Entry<String, String> entry : measureComorbidityMap.get(measure).entrySet()) {
                        if (comorbidityColumnsMap.containsKey(entry.getKey())) {
                            col = comorbidityColumnsMap.get(entry.getKey());

                            if (inputValues[COL_FIRST_COMORBIDITY+col].trim().equalsIgnoreCase("YES")) {
                                comorbiditySubScore++;
                            }
                        }
                    }

                    lengthOfStay = Integer.parseInt(inputValues[COL_LENGTH_OF_STAY]);

                    if (lengthOfStay < 1) {
                        lengthOfStayScore = 0;
                    } else if (lengthOfStay == 1) {
                        lengthOfStayScore = 1;
                    } else if (lengthOfStay == 2) {
                        lengthOfStayScore = 2;
                    } else if (lengthOfStay == 3) {
                        lengthOfStayScore = 3;
                    } else if (lengthOfStay >= 4 && lengthOfStay <= 6) {
                        lengthOfStayScore = 4;
                    } else if (lengthOfStay >= 7 && lengthOfStay <= 13) {
                        lengthOfStayScore = 5;
                    } else {
                        lengthOfStayScore = 7;
                    }

                    acuteAdmissions = Integer.parseInt(inputValues[COL_INPATIENT_VISITS]);

                    if (acuteAdmissions > 0) {
                        acuteAdmissionsScore = 3;
                    } else {
                        acuteAdmissionsScore = 0;
                    }

                    if (comorbiditySubScore == 0) {
                        comorbidityScore = 0;
                    } else if (comorbiditySubScore == 1) {
                        comorbidityScore = 1;
                    } else if (comorbiditySubScore == 2) {
                        comorbidityScore = 2;
                    } else if (comorbiditySubScore == 3) {
                        comorbidityScore = 3;
                    } else {
                        comorbidityScore = 5;
                    }

                    edVists = Integer.parseInt(inputValues[COL_ED_VISITS]);

                    if (edVists == 0) {
                        edVistsScore = 0;
                    } else if (edVists == 1) {
                        edVistsScore = 1;
                    } else if (edVists == 2) {
                        edVistsScore = 2;
                    } else if (edVists == 3) {
                        edVistsScore = 3;
                    } else {
                        edVistsScore = 4;
                    }

                    laceScore = lengthOfStayScore + acuteAdmissionsScore + comorbidityScore + edVistsScore;

                    if ( laceScore >= HIGH_LACE_SCORE_VALUE ) {
                        laceScoreFl = 'Y';
                    } else {
                        laceScoreFl = 'N';
                    }

                    outKey.clear();
                    outKey.set(measure);

                    outValueText.setLength(0);
                    outValueText.append(inputValues[COL_ENCOUNTER_ID]);
                    outValueText.append(Util.FIELD_SEPARATOR);
                    outValueText.append(inputValues[COL_DIAGNOSIS_CODE]);
                    outValueText.append(Util.FIELD_SEPARATOR);
                    outValueText.append(String.valueOf(lengthOfStay));
                    outValueText.append(Util.FIELD_SEPARATOR);
                    outValueText.append(String.valueOf(lengthOfStayScore));
                    outValueText.append(Util.FIELD_SEPARATOR);
                    outValueText.append(String.valueOf(acuteAdmissions));
                    outValueText.append(Util.FIELD_SEPARATOR);
                    outValueText.append(String.valueOf(acuteAdmissionsScore));
                    outValueText.append(Util.FIELD_SEPARATOR);
                    outValueText.append(String.valueOf(comorbiditySubScore));
                    outValueText.append(Util.FIELD_SEPARATOR);
                    outValueText.append(String.valueOf(comorbidityScore));
                    outValueText.append(Util.FIELD_SEPARATOR);
                    outValueText.append(String.valueOf(edVists));
                    outValueText.append(Util.FIELD_SEPARATOR);
                    outValueText.append(String.valueOf(edVistsScore));
                    outValueText.append(Util.FIELD_SEPARATOR);
                    outValueText.append(String.valueOf(laceScore));
                    outValueText.append(Util.FIELD_SEPARATOR);
                    outValueText.append(laceScoreFl);

                    outValue.clear();
                    outValue.set(outValueText.toString());

                    context.write(outKey, outValue);
                }
            }
        }
    }
}
