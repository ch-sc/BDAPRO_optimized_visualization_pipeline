package de.tuberlin.dima.bdapro.service;

import java.io.OutputStream;

import de.tuberlin.dima.bdapro.data.DataProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class DataService {
	
	@Autowired
	@Qualifier("data-processor.sequential")
	private DataProcessor sequentialDataProcessor;
	@Autowired
	@Qualifier("data-processor.parallel")
	private DataProcessor parallelDataProcessor;
	@Autowired
	@Qualifier("data-processor.flink")
	private DataProcessor streamDataProcessor;
	
	public int[][] scatterPlot(int x, int y) {
		return sequentialDataProcessor.scatterPlot(x, y);
	}
	
	
	public int[][] scatterPlot() {
		return sequentialDataProcessor.scatterPlot();
	}
	
	public void scatterPlot(int xDim, int yDim, OutputStream outputStream) {
		streamDataProcessor.scatterPlot(xDim, yDim);
	}
	
}
