package de.tuberlin.dima.bdapro.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import de.tuberlin.dima.bdapro.data.DataProcessor;
import de.tuberlin.dima.bdapro.data.GenericDataAccessor;
import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.data.taxi.*;
import de.tuberlin.dima.bdapro.model.ExecutionType;
import de.tuberlin.dima.bdapro.model.OptimizationType;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableConfigurationProperties(ServiceProperties.class)
@Configuration
@Slf4j
public class ServiceConfiguration {
	
	private static final int BOUND = 1_000_000;
	
	@Autowired
	private ServiceProperties properties;

	/*
    @Bean("data-processor.simpleStreaming")
    public DataProcessor streamingProcessor() {
    	return dataProcessor(ExecutionType.SIMPLESTREAMING, null);
    }*/
	
	
	@Bean("data-processor.flink")
	public DataProcessor dataAccessorFlink() {
		return dataProcessor(ExecutionType.FLINK, properties.getData());
	}
	
	
	@Bean("data-processor.sequential")
	public DataProcessor dataAccessorSequential() {
		return dataProcessor(ExecutionType.SEQUENTIAL, properties.getData());
	}
	
	
	@Bean("data-processor.parallel")
	public DataProcessor dataAccessorParallel() {
		return dataProcessor(ExecutionType.PARALLEL, properties.getData());
	}
	
	
	@Bean("data-processor.simpleStream")
	public StreamProcessor simpleStreamProcessor() {
		return streamProcessor(ExecutionType.STREAMING, OptimizationType.VDDA);
	}
	
	
	@Bean("data-processor.m4Stream")
	public StreamProcessor streamDataProcessor() {
		return streamProcessor(ExecutionType.STREAMING, OptimizationType.VDDA);
	}
	
	
	@Bean("data-processor.kMeansVDDA")
	public StreamProcessor kMeansVDDA() {
		return streamProcessor(ExecutionType.CLUSTERING, OptimizationType.VDDA);
	}
	
	
	@Bean("data-processor.kMeans")
	public StreamProcessor kMeans() {
		return streamProcessor(ExecutionType.CLUSTERING, OptimizationType.NONE);
	}
	
	
	private static DataProcessor dataProcessor(ExecutionType executionType, DataConfig config) {
		if (executionType == null) {
			executionType = ExecutionType.SEQUENTIAL;
		}
		
		DataProcessor dataProcessor;
		switch (executionType) {
			default:
			case SEQUENTIAL:
				dataProcessor = new SimpleDataProcessor(config, loadTaxiData(config));
				break;
			case PARALLEL:
				dataProcessor = new ParallelDataProcessor(loadTaxiData(config));
				break;
			case FLINK:
				// ToDo: set up Flink exec env properly
				dataProcessor = new FlinkDataProcessor(config, ExecutionEnvironment.getExecutionEnvironment());
				break;
		}
		
		return dataProcessor;
	}
	
	
	private StreamProcessor streamProcessor(ExecutionType executionType, OptimizationType optimizationType) {
		if (executionType == null) {
			executionType = ExecutionType.STREAMING;
		}
		
		// obtain execution environment and set setBufferTimeout to 1 to enable
		// continuous flushing of the output buffers (lowest latency)
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//				.setBufferTimeout(60000);
		// make parameters available in the web interface
		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(properties.getFlink().args);
		env.getConfig().setGlobalJobParameters(params);
//		setupInputStream(env, params);

//		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		switch (executionType) {
			default:
			case STREAMING:
				if (optimizationType == OptimizationType.VDDA) {
					return new VDDAProcessor(env);
				} else {
					return new StreamDataProcessor(env);
				}
			case CLUSTERING:
				if (optimizationType == OptimizationType.VDDA) {
					return new KMeansVDDA(env);
				} else {
					return new KMeansSimple(env);
				}
		}
	}
	
	
	private static TaxiRide loadTaxiData(DataConfig config) {
		// ToDo: switch to a more generic approach
		// ToDo: load data from different data sources -> use JClouds or Hadoop
		
		GenericDataAccessor<TaxiRide> dataAccessor = new GenericDataAccessor<>(new File(config.getDataLocation()));
		dataAccessor.loadData(TaxiRide::loadData);
		return new TaxiRide(dataAccessor, dataAccessor.getCursor() + 0, dataAccessor.getLength());
	}
	
	
	private void setupInputStream(StreamExecutionEnvironment env, ParameterTool params) {
		DataStream<Tuple2<Integer, Integer>> inputStream;
		if (params.has("input")) {
			inputStream = env.readTextFile(params.get("input")).map(new FibonacciInputMap());
		} else {
			log.info("Executing Iterate example with default input data set.");
			log.info("Use --input to specify file input.");
			inputStream = env.addSource(new RandomFibonacciSource());
		}
		
		// create an iterative data stream from the input with 5 second timeout
		IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> it = inputStream.map(new InputMap())
				.iterate(5000);
		
		// apply the step function to get the next Fibonacci number
		// increment the counter and split the output with the output selector
		SplitStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> step = it.map(new Step())
				.split(new MySelector());
		
		// close the iteration by selecting the tuples that were directed to the
		// 'iterate' channel in the output selector
		it.closeWith(step.select("iterate"));
		
		// to produce the final output select the tuples directed to the
		// 'output' channel then get the input pairs that have the greatest iteration counter
		// on a 1 second sliding window
		DataStream<Tuple2<Tuple2<Integer, Integer>, Integer>> numbers = step.select("output").map(new OutputMap());
		
		// emit results
		if (params.has("output")) {
			numbers.writeAsText(params.get("output"));
		} else {
			log.info("Printing result to stdout. Use --output to specify output path.");
			numbers.print();
		}
	}
	
	
	/**
	 * Generate random integer pairs with range 0 to {@value BOUND}.
	 */
	private static class FibonacciInputMap implements MapFunction<String, Tuple2<Integer, Integer>> {
		
		private static final long serialVersionUID = 1L;
		
		
		@Override
		public Tuple2<Integer, Integer> map(String value) throws Exception {
			String record = value.substring(1, value.length() - 1);
			String[] splitted = record.split(",");
			return new Tuple2<>(Integer.parseInt(splitted[0]), Integer.parseInt(splitted[1]));
		}
	}
	
	
	/**
	 * Map the inputs so that the next Fibonacci numbers can be calculated while preserving the original input tuple.
	 * A counter is attached to the tuple and incremented in every iteration step.
	 */
	public static class InputMap implements MapFunction<Tuple2<Integer, Integer>, Tuple5<Integer, Integer, Integer,
			Integer, Integer>> {
		
		private static final long serialVersionUID = 1L;
		
		
		@Override
		public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws
				Exception {
			return new Tuple5<>(value.f0, value.f1, value.f0, value.f1, 0);
		}
	}
	
	
	/**
	 * Iteration step function that calculates the next Fibonacci number.
	 */
	public static class Step implements
			MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer,
					Integer, Integer>> {
		
		private static final long serialVersionUID = 1L;
		
		
		@Override
		public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer,
				Integer> value) throws Exception {
			return new Tuple5<>(value.f0, value.f1, value.f3, value.f2 + value.f3, ++value.f4);
		}
	}
	
	
	/**
	 * OutputSelector testing which tuple needs to be iterated again.
	 */
	public static class MySelector implements OutputSelector<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
		
		private static final long serialVersionUID = 1L;
		
		
		@Override
		public Iterable<String> select(Tuple5<Integer, Integer, Integer, Integer, Integer> value) {
			List<String> output = new ArrayList<>();
			if (value.f2 < BOUND && value.f3 < BOUND) {
				output.add("iterate");
			} else {
				output.add("output");
			}
			return output;
		}
	}
	
	
	/**
	 * Giving back the input pair and the counter.
	 */
	public static class OutputMap implements MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,
			Tuple2<Tuple2<Integer, Integer>, Integer>> {
		
		private static final long serialVersionUID = 1L;
		
		
		@Override
		public Tuple2<Tuple2<Integer, Integer>, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer> value)
				throws Exception {
			return new Tuple2<>(new Tuple2<>(value.f0, value.f1), value.f4);
		}
	}
	
	
	/**
	 * Generate random integer pairs of range 0 to {@value BOUND}
	 */
	private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {
		
		private static final long serialVersionUID = 1L;
		
		private Random rnd = new Random();
		
		private volatile boolean isRunning = true;
		private int counter = 0;
		
		
		@Override
		public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			
			while (isRunning && counter < BOUND) {
				int first = rnd.nextInt(BOUND);
				int second = rnd.nextInt(BOUND);
				
				ctx.collect(new Tuple2<>(first, second));
				counter++;
				Thread.sleep(50L);
			}
		}
		
		
		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}
