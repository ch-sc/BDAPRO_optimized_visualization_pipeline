package de.tuberlin.dima.bdapro.service;

import de.tuberlin.dima.bdapro.data.DataProcessor;
import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.Point;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

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
	@Autowired
	@Qualifier("data-processor.m4Stream")
	private StreamProcessor streamProcessor;
	@Autowired
	@Qualifier("data-processor.simpleStream")
	private StreamProcessor simpleStreamProcessor;
	@Autowired
	@Qualifier("data-processor.kMeansVDDA")
	private StreamProcessor kMeansVDDA;
	@Autowired
	@Qualifier("data-processor.kMeans")
	private StreamProcessor kMeans;
	
	
	public int[][] scatterPlot(int x, int y) {
		return sequentialDataProcessor.scatterPlot(x, y);
	}
	
	
	public int[][] scatterPlot() {
		return sequentialDataProcessor.scatterPlot();
	}

	public DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> streamingScatterPlot(int x, int y, Time window, Time slide) { return streamProcessor.scatterPlot(x,y, window, slide); }

	public DataStream<Tuple3<LocalDateTime, Point, ClusterCenter>> cluster(int x, int y, int k, int maxIter, Time window, Time slide) { return streamProcessor.cluster(x,y, k, maxIter, window, slide); }

	
}
