package de.tuberlin.dima.bdapro.service;

import de.tuberlin.dima.bdapro.config.ServiceConfiguration;
import de.tuberlin.dima.bdapro.data.DataProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class DataService {
	
	@Autowired
	private ServiceConfiguration serviceConfiguration;
	
	@Autowired
	@Qualifier("data-processor.sequential")
	private DataProcessor dataProcessor;
	
	
	public int[][] scatterPlot(int x, int y){
		return dataProcessor.scatterPlot(x, y);
	}
}
