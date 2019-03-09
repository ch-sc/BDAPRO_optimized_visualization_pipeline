package de.tuberlin.dima.bdapro.util;

import java.util.ArrayList;
import java.util.List;

public class DataTransformer {
	
	
	/**
	 * Transforms an integer n*m matrix into an array of 3-tuples (x,y,value).
	 *
	 * @param grid
	 * @return
	 */
	public static Object[] gridToCoordinates(int[][] grid) {
		List<int[]> list = new ArrayList<>(grid.length);
		
		for (int i = 0; i < grid.length; i++) {
			final int[] row = grid[i];
			for (int j = 0; j < row.length; j++) {
				if (row[j] == 0) {
					continue;
				}
				list.add(new int[] { i, j, row[j] });
			}
		}
		return list.toArray();
	}
}
