package de.tuberlin.dima.bdapro.service;

import de.tuberlin.dima.bdapro.data.DataProcessor;
import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.ExecutionType;
import de.tuberlin.dima.bdapro.model.Point;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
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
	private StreamProcessor m4StreamProcessor;
	@Autowired
	@Qualifier("data-processor.simpleStream")
	private StreamProcessor simpleStreamProcessor;
	@Autowired
	@Qualifier("data-processor.kMeansVDDA")
	private StreamProcessor kMeansVDDAProcessor;
	@Autowired
	@Qualifier("data-processor.kMeans")
	private StreamProcessor kMeansProcessor;
	
	
	public int[][] scatterPlot(int x, int y) {
		return sequentialDataProcessor.scatterPlot(x, y);
	}
	
	
	public int[][] scatterPlot() {
		return sequentialDataProcessor.scatterPlot();
	}
	
	
	public DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> streamingScatterPlot(int x, int y) {
		return m4StreamProcessor
				.scatterPlot(x, y);
	}
	
	
	public void clusterAsync(ExecutionType execType, int x, int y, int k, int maxIter) {
		StreamProcessor streamProcessor = selectStreamProcessor(execType);
		
		DataStream<Tuple3<LocalDateTime, Point, ClusterCenter>> clusterStream =
				streamProcessor.cluster(x, y, k, maxIter);
		
		
//		clusterStream.windo
//
//		clusterStream.(new MapFunction<Tuple3<LocalDateTime, Point, ClusterCenter>, Object[]>() {
//			@Override
//			public Object[] map(Tuple3<LocalDateTime, Point, ClusterCenter> localDateTimePointClusterCenterTuple3)
//					throws Exception {
//				return new Object[0];
//			}
//		})
	}
	
	
	private StreamProcessor selectStreamProcessor(ExecutionType execType) {
		
		StreamProcessor streamProcessor;
		
		switch (execType) {
			default:
			case SEQUENTIAL:
			case PARALLEL:
			case FLINK:
				throw new NotImplementedException(
						"SEQUENTIAL and PARALLEL and FLINK execution environments are not supported for async clustering");
			case SIMPLESTREAMING:
				streamProcessor = simpleStreamProcessor;
				break;
			case M4STREAMING:
				streamProcessor = m4StreamProcessor;
				break;
			case KMEANSVDDA:
				streamProcessor = kMeansVDDAProcessor;
				break;
			case KMEANS:
				streamProcessor = kMeansProcessor;
				break;
		}
		
		return streamProcessor;
	}
}
