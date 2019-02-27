package de.tuberlin.dima.bdapro.model;

import java.io.Serializable;
import java.util.Arrays;

/**
 *
 */

public class Point implements Serializable {

    private double fields[];

    public Point(int dimensions) {
        this.fields = new double[dimensions];
    }

    public Point(double fields[]) {
        this.fields = fields;
    }

    public double[] getFields() {
        return fields;
    }

    public void setFields(double[] fields) {
        this.fields = fields;
    }

    public double euclideanDistance(Point other) {
        double distance = 0;
        for (int i = 0; i < fields.length; i++) {
            distance += (fields[i] - other.fields[i]) * (fields[i] - other.fields[i]);
        }
        return Math.sqrt(distance);
    }

    public Point add (Point other){
        for (int i = 0; i < fields.length; i++) {
            fields[i] += other.fields[i];
        }
        return this;
    }

    public Point divideByScalar (long val){
        for (int i = 0; i < fields.length; i++) {
            fields[i] /= val;
        }
        return this;
    }

    @Override
    public String toString() {
        return Arrays.toString(fields);
    }
}

