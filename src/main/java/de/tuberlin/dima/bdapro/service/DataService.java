package de.tuberlin.dima.bdapro.service;

import java.io.OutputStream;

import de.tuberlin.dima.bdapro.data.DataProcessor;
import de.tuberlin.dima.bdapro.data.taxi.StreamDataProcessor;
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
	private DataProcessor flinkDataProcessor;
	//@Autowired
	//@Qualifier("data-processor.simpleStream")
	//private StreamDataProcessor streamDataProcessor;
	
	
	public int[][] scatterPlot(int x, int y) {
		return sequentialDataProcessor.scatterPlot(x, y);
	}
	
	
	public int[][] scatterPlot() {
		return sequentialDataProcessor.scatterPlot();
	}

	
}
