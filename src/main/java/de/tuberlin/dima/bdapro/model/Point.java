package de.tuberlin.dima.bdapro.model;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Class that defines a point
 */

public class Point implements Serializable {
	
	private double fields[];
	
	
	/**
	 * initialize the dimension point without any coordinates
	 *
	 * @param dimensions int for the number of dimensions of the point
	 */
	public Point(int dimensions) {
		this.fields = new double[dimensions];
	}
	
	
	/**
	 * Initializes a point with coordinates
	 *
	 * @param fields coordinates of the point
	 */
	public Point(double fields[]) {
		this.fields = fields;
	}
	
	
	/**
	 * method to get the coordinates of a point
	 *
	 * @return coordinates of a point
	 */
	public double[] getFields() {
		return fields;
	}
	
	
	/**
	 * method to set the coordinates of a point
	 *
	 * @param fields coordinates of a point
	 */
	public void setFields(double[] fields) {
		this.fields = fields;
	}
	
	
	/**
	 * calculate the euclidean distance between two points
	 *
	 * @param other the other point to calculate the distance with
	 * @return retuns the euclidean distance between two points
	 */
	public double euclideanDistance(Point other) {
		double distance = 0;
		for (int i = 0; i < fields.length; i++) {
			distance += (fields[i] - other.fields[i]) * (fields[i] - other.fields[i]);
		}
		return Math.sqrt(distance);
	}
	
	
	/**
	 * write the point as string
	 *
	 * @return string value of the coordinates
	 */
	@Override
	public String toString() {
		return Arrays.toString(fields);
	}
}

