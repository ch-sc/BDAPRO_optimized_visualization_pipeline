package de.tuberlin.dima.bdapro.model.dto;

import lombok.Data;

@Data
public class TwoDimensionalPlotSto {
	
	/**
	 * contains data of format: [[1,2,3],[1,2,3],[1,2,3],...]
	 */
	private int[][] data;
	

}
