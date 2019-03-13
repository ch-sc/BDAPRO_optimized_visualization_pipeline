package de.tuberlin.dima.bdapro.model;

/**
 * Class for the cluster centers
 */

public class ClusterCenter extends Point {
	
	public int id;
	
	
	/**
	 * returns a cluster center
	 *
	 * @param id the id of the cluster center
	 * @param data an array of doubles with the coordinates of the cluster center
	 */
	public ClusterCenter(int id, double[] data) {
		super(data);
		this.id = id;
	}
	
	
	/**
	 * creates a new cluster center made from a point
	 *
	 * @param id the id of the cluster center
	 * @param p point with the coordinates of the cluster center
	 */
	
	public ClusterCenter(int id, Point p) {
		super(p.getFields());
		this.id = id;
	}
	
	
	/**
	 * get ID of cluster center
	 *
	 * @return ID of cluster center
	 */
	
	public int getId() {
		return id;
	}
	
	
	/**
	 * set ID of cluster center
	 *
	 * @param id id for the cluster center
	 */
	public void setId(int id) {
		this.id = id;
	}
	
	
	/**
	 * Generates a string with the id and the coordinates
	 *
	 * @return string with id and coordinates
	 */
	@Override
	public String toString() {
		return id + " " + super.toString();
	}
	
}

