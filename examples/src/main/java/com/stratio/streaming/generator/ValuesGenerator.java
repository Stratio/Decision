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
package com.stratio.streaming.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class ValuesGenerator {
    private List<RangeData> ranges;
    private double valueDeviation = 0;

    private double initValue = 0;

    public ValuesGenerator() {
        this.ranges = new LinkedList<>();
    }

    public ValuesGenerator(double initValue) {
        this();
        this.initValue = initValue;
    }

    public List<Double> build() {
        List<Double> values = new ArrayList<>();

        // Iteramos nuestros rangos
        for (RangeData range : ranges) {
            double min;
            double max;
            boolean reverse = false;
            if (initValue > range.getFinalValue()) {
                min = range.getFinalValue();
                max = initValue;
                // reverse result
                reverse = true;
            } else {
                max = range.getFinalValue();
                min = initValue;
            }
            // Lista de numeros
            List<Double> temporalValues = new ArrayList<>();
            for (double i = min; i < max; i++) {
                temporalValues.add(i);
            }
            List<Double> fixedNumberOfValues = new ArrayList<>();
            if (temporalValues.size() != 0) {
                Random r = new Random();
                for (int i = 0; i < range.getAffectedValues(); i++) {
                    Double randomElement = temporalValues.get(r.nextInt(temporalValues.size()));
                    fixedNumberOfValues.add(randomElement);
                }
            }
            Collections.sort(fixedNumberOfValues);
            if (reverse) {
                Collections.reverse(fixedNumberOfValues);
            }
            for (Double value : fixedNumberOfValues) {
                values.add(rand(value - valueDeviation, value + valueDeviation));
            }
            initValue = range.getFinalValue();
        }
        printValues(values);
        return values;
    }

    public ValuesGenerator withDerivation(int derivation) {
        this.valueDeviation = derivation;
        return this;
    }

    public ValuesGenerator withInitValue(double initValue) {
        this.initValue = initValue;
        return this;
    }

    public ValuesGenerator addRange(double finalValue, int affectedValues) {
        ranges.add(new RangeData(finalValue, affectedValues));
        return this;
    }

    private void printValues(List<Double> values) {
        int valuesNumber = values.size();
        int groupValuesSize = valuesNumber / 15;
        int count = 0;
        Double avg = 0d;
        Double o = 50f / Collections.max(values);
        for (Double value : values) {
            avg = avg + value;
            count++;
            if (count == groupValuesSize) {
                for (int i = 0; i < (o * (avg / count)); i++) {
                    System.out.print("█");
                }
                System.out.println(" (" + avg / count + ")");
                avg = 0d;
                count = 0;
            }
        }

    }

    private double rand(double min, double max) {

        // Usually this can be a field rather than a method variable
        Random rand = new Random();

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt(((int) max - (int) min) + 1) + (int) min;

        return (Double.valueOf(randomNum) < 0) ? 0 : Double.valueOf(randomNum);
    }

    private class RangeData {
        private final double finalValue;
        private final int affectedValues;

        public RangeData(double finalValue, int affectedValues) {
            this.finalValue = finalValue;
            this.affectedValues = affectedValues;
        }

        public double getFinalValue() {
            return finalValue;
        }

        public int getAffectedValues() {
            return affectedValues;
        }

    }
}
