package de.tuberlin.dima.bdapro.data;

import de.tuberlin.dima.bdapro.model.Point;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class DataProcessor {
	
	abstract public int[][] scatterPlot(int xBound, int yBound);
	
	abstract public int[][] scatterPlot();
	
}


