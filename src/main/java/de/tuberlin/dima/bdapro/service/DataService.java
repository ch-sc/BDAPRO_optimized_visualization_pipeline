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
import de.tuberlin.dima.bdapro.model.Point;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@Slf4j
public class DataService {
	
	@Autowired
	private MessagingService messagingService;

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


	public int[][] scatterPlot(ExecutionType executionType, int x, int y) {
		return selectDataProcessor(executionType)
				.scatterPlot(x, y);
	}


	public int[][] scatterPlot(ExecutionType executionType) {
		return selectDataProcessor(executionType)
				.scatterPlot();
	}

	
	public void scatterPlotAsync(ExecutionType execType, int x, int y, Time window, Time slide) {
		StreamProcessor streamProcessor = selectStreamProcessor(execType);

		DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> scatterPlotStream =
				streamProcessor.scatterPlot(x, y, window, slide);
	}
	
	
	public void clusterAsync(ExecutionType execType, int x, int y, int k, int maxIter, Time window, Time slide) {
		StreamProcessor streamProcessor = selectStreamProcessor(execType);

		DataStream<Tuple3<LocalDateTime, Point, ClusterCenter>> clusterStream =
				streamProcessor.cluster(x, y, k, maxIter, window, slide);

		RMQSink<Object[]> sink = getSink();

		DataStreamSink dataStreamSink = clusterStream
				.keyBy(0)
				.windowAll(TumblingEventTimeWindows.of(window))
//				.countWindow(1000)
				.aggregate(preAggregate)
				.addSink(sink);
//				.process(new SideOutProcess(messagingService, outputTag));

		try {
			streamProcessor.run();
		} catch (Exception e) {
			throw new BusinessException("Flink job throw an error: " + ExceptionUtils.getMessage(e), e);
		}

//		DataStream<Object[]> sideOutputStream = mainDataStream.getSideOutput(outputTag);
	}


	private RMQSink<Object[]> getSink() {
		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
				.setHost("localhost")
				.setVirtualHost("MyRabbit")
				.setPort(5672)
				.setUserName("user")
				.setPassword("password")
				.build();

		return new RMQSink<>(
				connectionConfig,            // config for the RabbitMQ connection
				"BDAPRO",                    // name of the RabbitMQ queue to send messages to
				new SerializationSchemaImpl()
		);
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
			case VDDASTREAMING:
				streamProcessor = vddaStreamProcessor;
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


	private DataProcessor selectDataProcessor(ExecutionType execType) {
		switch (execType) {
			case SEQUENTIAL:
				return sequentialDataProcessor;
			case PARALLEL:
				return parallelDataProcessor;
			case FLINK:
				return flinkDataProcessor;
			case SIMPLESTREAMING:
			case VDDASTREAMING:
			case KMEANSVDDA:
			case KMEANS:
			default:
				throw new NotImplementedException(
						"SEQUENTIAL and PARALLEL and FLINK execution environments are not supported for async clustering");
		}
	}


	private AggregateFunction preAggregate =
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


	@AllArgsConstructor
	public static class SideOutProcess extends ProcessFunction<Object[], Object[]> implements Serializable {

		final private MessagingService messagingService;
		final private OutputTag<Object[]> outputTag;


		@Override
		public void processElement(Object[] dataPoints, Context ctx, Collector<Object[]> collector) throws Exception {
			// emit data to regular output
			collector.collect(dataPoints);

			// send processed data to queue
			messagingService.send(MessagingService.CLUSTER_DATAPOINT, dataPoints);

			// emit data to side output
			ctx.output(outputTag, dataPoints);

		}
	}

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
