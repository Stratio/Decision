/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.decision.unit.siddhi.query;

import au.com.bytecode.opencsv.CSVReader;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

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
