package com.stratio.streaming.unit.siddhi.query;

import au.com.bytecode.opencsv.CSVReader;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by aitor on 9/17/15.
 */
public class ResourcesLoader {

    private static final char SPLIT_BY= ';';
    private static final char QUOTE= '\0'; // By default empty quote char, the common alternative is '"'
    private static final int START_FROM_LINE= 1;

    public static List<String[]> loadData(String fileName)    {
        return loadData(fileName, SPLIT_BY, QUOTE, START_FROM_LINE);
    }

    /**
     * Return a List with the contents of a given CSV with supplied separator and quote char.
     *
     * @param fileName The path to the CSV to parse
     * @param splitBy The delimiter to use for separating entries
     * @param quoteChar The character to use for quoted elements
     * @param startFromLine The line number to skip for start reading
     */
    public static List<String[]> loadData(String fileName, char splitBy, char quoteChar, int startFromLine) {
        List<String[]> contents = new ArrayList<>();

        try {
            CSVReader reader= new CSVReader(new FileReader(fileName), splitBy, quoteChar, startFromLine);
            String[] nextLine;

            int counter= 0;

            while ((nextLine = reader.readNext()) != null) {
                if (nextLine != null && nextLine.length > 0) {
                    try {
                        contents.add(nextLine);
                        //contents.put(counter, nextLine);
                        //counter++;
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return contents;
    }

}
