package de.tuberlin.dima.bdapro.service;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tuberlin.dima.bdapro.data.DataProcessor;
import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.error.BusinessException;
import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.ExecutionType;
import de.tuberlin.dima.bdapro.model.OptimizationType;
import de.tuberlin.dima.bdapro.model.Point;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.OutputTag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DataService {
	
	final OutputTag<Object[]> outputTag = new OutputTag<Object[]>("side-output") {
	};
	
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
	private StreamProcessor vddaStreamProcessor;
	@Autowired
	@Qualifier("data-processor.simpleStream")
	private StreamProcessor simpleStreamProcessor;
	@Autowired
	@Qualifier("data-processor.kMeansVDDA")
	private StreamProcessor kMeansVDDAProcessor;
	@Autowired
	@Qualifier("data-processor.kMeans")
	private StreamProcessor kMeansProcessor;
	
	@Autowired
	private RMQConnectionConfig rmqConnectionConfig;
	
	/**
	 * Creates a two-dimensional data grid on the experiment data while reducing data within given boundaries. The
	 * execution type determines what implementation to use.
	 *
	 * @param executionType type of execution
	 * @param x boundary of the first dimension
	 * @param y boundary of the second dimension
	 * @return a two-dimensional integer matrix containing the number of data points per entry
	 */
	public int[][] scatterPlot(ExecutionType executionType, int x, int y) {
		return selectDataProcessor(executionType)
				.scatterPlot(x, y);
	}
	
	
	/**
	 * Creates a two-dimensional data grid based on the experiment data without any data reduction. The execution type
	 * determines what implementation to use.
	 *
	 * @param executionType type of execution
	 * @return a two-dimensional integer matrix containing the number of data points per entry
	 */
	public int[][] scatterPlot(ExecutionType executionType) {
		return selectDataProcessor(executionType)
				.scatterPlot();
	}
	
	
	/**
	 * Invokes Flink streaming on the experiment data set to build data points in a two-dimensional scatter plot.
	 * Results are  written to the message broker.
	 *
	 * @param optimizationType optimization type
	 * @param x boundary of the first dimension
	 * @param y boundary of the second dimension
	 * @param window window size for streaming over the input data
	 * @param slide window shift
	 */
	public void doScatterStreaming(OptimizationType optimizationType, int x, int y, Time window, Time slide) {
		StreamProcessor streamProcessor = selectStreamProcessor(ExecutionType.STREAMING, optimizationType);
		
		DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> scatterPlotStream =
				streamProcessor.scatterPlot(x, y, window, slide);
	}
	
	
	/**
	 * Invokes Flink streaming on the experiment data set, building data points for a two-dimensional scatter plot and
	 * clusters these data points. Results are written to the message broker.
	 *
	 * @param optimizationType optimization type
	 * @param x bounday of the first dimension
	 * @param y bounday of the second dimension
	 * @param k number of clusters
	 * @param maxIter maximum number of iterations in k-means
	 * @param window window size for streaming over the input data
	 * @param slide window shift
	 */
	public void cluster(OptimizationType optimizationType, int x, int y, int k, int maxIter, Time window, Time slide) {
		StreamProcessor streamProcessor = selectStreamProcessor(ExecutionType.CLUSTERING, optimizationType);
		
		DataStream<Tuple3<LocalDateTime, Point, ClusterCenter>> clusterStream =
				streamProcessor.cluster(x, y, k, maxIter, window, slide);
		
		clusterStream
				.keyBy(0)
				.windowAll(TumblingEventTimeWindows.of(window))
				.aggregate(catchTuples)
				.addSink(rabbitMqSink());
		
		try {
			streamProcessor.run();
		} catch (Exception e) {
			throw new BusinessException("Flink job threw an error: " + ExceptionUtils.getMessage(e), e);
		}

	}
	
	
	private RMQSink<Object[]> rabbitMqSink() {
		return new RMQSink<>(rmqConnectionConfig, "BDAPRO2", new SerializationSchemaImpl());
	}
	
	
	private StreamProcessor selectStreamProcessor(ExecutionType execType, OptimizationType optimizationType) {
		
		switch (execType) {
			default:
			case SEQUENTIAL:
			case PARALLEL:
			case FLINK:
				throw new NotImplementedException(
						"execution type " + execType + " not supported for any Flink execution environment");
			case STREAMING:
				if (optimizationType == OptimizationType.VDDA) {
					return vddaStreamProcessor;
				} else {
					return simpleStreamProcessor;
				}
			case CLUSTERING:
				if (optimizationType == OptimizationType.VDDA) {
					return kMeansVDDAProcessor;
				} else {
					return kMeansProcessor;
				}
		}
	}
	
	
	private DataProcessor selectDataProcessor(ExecutionType execType) {
		switch (execType) {
			case SEQUENTIAL:
				return sequentialDataProcessor;
			case PARALLEL:
				return parallelDataProcessor;
			case FLINK:
				return flinkDataProcessor;
			case STREAMING:
			case CLUSTERING:
			default:
				throw new NotImplementedException(
						"execution type " + execType + " not supported for any vanilla Java data processing");
		}
	}
	
	
	private AggregateFunction catchTuples =
			new AggregateFunction<Tuple3<LocalDateTime, Point, ClusterCenter>, List<int[]>, Object[]>() {
				@Override
				public List<int[]> createAccumulator() {
					return new ArrayList<>(1000);
				}
				
				
				@Override
				public List<int[]> add(
						Tuple3<LocalDateTime, Point, ClusterCenter> tuple,
						List<int[]> acc) {
					Point dp = tuple.f1;
					ClusterCenter cluster = tuple.f2;
					double[] dpPos = dp.getFields();
					int[] entry = { (int) dpPos[0], (int) dpPos[1], cluster.getId() };
					acc.add(entry);
					return acc;
				}
				
				
				@Override
				public Object[] getResult(List<int[]> integers) {
					return integers.toArray();
				}
				
				
				@Override
				public List<int[]> merge(List<int[]> acc1, List<int[]> acc2) {
					acc1.addAll(acc2);
					return acc1;
				}
			};
	
	public static class SerializationSchemaImpl implements SerializationSchema<Object[]>, Serializable {
		
		ObjectMapper mapper = new ObjectMapper();
		
		
		@Override
		public byte[] serialize(Object[] objects) {
			try {
				return mapper.writeValueAsBytes(objects);
			} catch (JsonProcessingException e) {
//							log.error("Could not serialize data: " + e.getMessage());
				return null;
			}
		}
	}
}
