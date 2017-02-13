package com.relsulla.sample_data.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Bob on 2/10/2017.
 */
public class TestLookups {

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

    private HashMap<String, Integer> comorbidityColumnsMap = new HashMap<String, Integer>();
    private HashMap<String,String> diagnosisCodesMap = new HashMap<String, String>();
    private HashMap<String, HashMap<String,String>> measureComorbidityMap = new HashMap<String, HashMap<String,String>>();

    private int maxCol = 0;
    private int row = 2;

    public static void main(String[] args) {

        TestLookups instance = new TestLookups();

        instance.run(args[0],args[1],args[2],args[3], args[4]);
    }

    private void run(String comorbidityColumnsFile
                    ,String diagnosisCodesFile
                    ,String measureComorbidityFile
                    ,String dataFile
                    ,String selectedMeasure) {

        try {
            getComorbidityColumnsMap(comorbidityColumnsFile);
            getDiagnosisCodesMap(diagnosisCodesFile);
            getMeasureComorbidityMap(measureComorbidityFile);


            String[] inputValues;
            String line;

            BufferedReader br = new BufferedReader(new FileReader(dataFile));

            while ( (line = br.readLine()) != null ) {
                inputValues = line.split(",",-1);
                proc(inputValues,selectedMeasure);
                row++;
            }

            br.close();

        } catch(Exception ex) {
            ex.printStackTrace();
            System.exit(8);
        }

    }

    private void proc(String[] inputValues
                     ,String selectedMeasure) {

        int col;
        int lengthOfStay;
        int acuteAdmissions;
        int edVists;

        String measure;

        int lengthOfStayScore = 0;
        int acuteAdmissionsScore = 0;
        int comorbiditySubScore = 0;
        int comorbidityScore = 0;
        int edVistsScore = 0;

        try {
            lengthOfStay = Integer.parseInt(inputValues[COL_LENGTH_OF_STAY]);
        } catch (Exception ex ) {
            lengthOfStay = -1;
        }

        try {
            acuteAdmissions = Integer.parseInt(inputValues[COL_INPATIENT_VISITS]);
        } catch (Exception ex) {
            acuteAdmissions = -1;
        }

        try {
            edVists = Integer.parseInt(inputValues[COL_ED_VISITS]);
        } catch (Exception ex) {
            edVists = -1;
        }

        if ( inputValues.length >= (COL_FIRST_COMORBIDITY + maxCol) && lengthOfStay >= 0 && acuteAdmissions >= 0 && edVists >= 0 ) {
            if (diagnosisCodesMap.containsKey(inputValues[COL_DIAGNOSIS_CODE]) ) {
                measure = diagnosisCodesMap.get(inputValues[COL_DIAGNOSIS_CODE]);
            } else {
                measure = "##ERR";
            }

            if ( measure.equals(selectedMeasure) && measureComorbidityMap.containsKey(selectedMeasure)) {
                for (Map.Entry<String, String> entry : measureComorbidityMap.get(selectedMeasure).entrySet()) {
                    if (comorbidityColumnsMap.containsKey(entry.getKey())) {
                        col = comorbidityColumnsMap.get(entry.getKey());

                        if (inputValues[col].trim().equalsIgnoreCase("YES")) {
                            comorbiditySubScore++;
                        }
                    }
                }

                if ( lengthOfStay < 1 ) {
                    lengthOfStayScore = 0;
                } else if ( lengthOfStay == 1 ) {
                    lengthOfStayScore = 1;
                } else if ( lengthOfStay == 2 ) {
                    lengthOfStayScore = 2;
                } else if ( lengthOfStay == 3 ) {
                    lengthOfStayScore = 3;
                } else if ( lengthOfStay >= 4 && lengthOfStay <= 6 ) {
                    lengthOfStayScore = 4;
                } else if ( lengthOfStay >= 7 && lengthOfStay <= 13 ) {
                    lengthOfStayScore = 5;
                } else {
                    lengthOfStayScore = 7;
                }

                if ( acuteAdmissions > 0 ) {
                    acuteAdmissionsScore = 3;
                } else {
                    acuteAdmissionsScore = 0;
                }

                if ( comorbiditySubScore == 0 ) {
                    comorbidityScore = 0;
                } else if ( comorbiditySubScore == 1 ) {
                    comorbidityScore = 1;
                } else if ( comorbiditySubScore == 2 ) {
                    comorbidityScore = 2;
                } else if ( comorbiditySubScore == 3 ) {
                    comorbidityScore = 3;
                } else {
                    comorbidityScore = 5;
                }

                if ( edVists == 0 ) {
                    edVistsScore = 0;
                } else if ( edVists == 1 ) {
                    edVistsScore = 1;
                } else if ( edVists == 2 ) {
                    edVistsScore = 2;
                } else if ( edVists == 3 ) {
                    edVistsScore = 3;
                } else {
                    edVistsScore = 4;
                }
            }

            System.out.println((row) + "|" +
                    inputValues[COL_ENCOUNTER_ID] + "|" + inputValues[COL_DIAGNOSIS_CODE] + "|" + measure
                    + lengthOfStay + "|" + lengthOfStayScore + "|"
                    + acuteAdmissions + "|" + acuteAdmissionsScore + "|"
                    + comorbiditySubScore + "|" + comorbidityScore + "|"
                    + edVists + "|" + edVistsScore);
        }

    }

    private void getComorbidityColumnsMap(String comorbidityColumnsFile) {

        BufferedReader br;
        String line;
        int idx = 0;

        try {
            br = new BufferedReader(new FileReader(comorbidityColumnsFile));

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
            br = new BufferedReader(new FileReader(diagnosisCodesFile));

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
            br = new BufferedReader(new FileReader(measureComorbidityFile));

            while ((line = br.readLine()) != null) {
                lineParts = line.split("\\t");
                parts = lineParts[1].split(",");

                HashMap<String,String> comorbidityMap = new HashMap<String,String>();

                for (int idx = 0; idx < parts.length; idx++) {
                    comorbidityMap.put(parts[idx].trim(), "");
                }

                measureComorbidityMap.put(lineParts[0], comorbidityMap);
            }

            br.close();
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(8);
        }
    }

}
