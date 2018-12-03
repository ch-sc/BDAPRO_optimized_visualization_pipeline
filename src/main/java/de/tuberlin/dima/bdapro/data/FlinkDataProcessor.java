package de.tuberlin.dima.bdapro.data;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import de.tuberlin.dima.bdapro.config.AppConfig;
import de.tuberlin.dima.bdapro.error.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

@Slf4j
public class FlinkDataProcessor extends DataProcessor {
	
	private AppConfig config;
	private ExecutionEnvironment env;
	
	
	public FlinkDataProcessor(AppConfig config, ExecutionEnvironment env) {
		this.config = config;
		this.env = env;
	}
	
	public int[][] scatterPlot(int xBound, int yBound) {
		CsvReader csvReader = env.readCsvFile(config.getDataLocation())
				.ignoreFirstLine()
				.ignoreInvalidLines()
				.fieldDelimiter(',')
				.includeFields(createFilterMask(4, 13)); // 4: distance, 13: fare
		
		DataSet<Tuple2<Double, Double>> data = csvReader.types(Double.class, Double.class)
				.project(0, 1);
		
		StopWatch timer = new StopWatch();
		timer.start();
		
		try {
			List<Tuple2<Double, Double>> maxValuesCollection = data
					.reduce((ReduceFunction<Tuple2<Double, Double>>) (t1, t2) -> new Tuple2<>(Math.max(t1.f0, t2.f0),
							Math.max(t1.f1, t2.f1)))
					.collect();
			
			final Tuple2<Double, Double> maxValues = maxValuesCollection.get(0);
			
			
			
			List<Integer[][]> plotPoints = data
					.reduceGroup(new GroupReduceFunction<Tuple2<Double, Double>, Tuple2<Integer, Integer>>() {
						@Override
						public void reduce(Iterable<Tuple2<Double, Double>> iterable,
								Collector<Tuple2<Integer, Integer>> collector) {
							iterable.forEach(t -> {
								collector.collect(new Tuple2<>(
										Math.max(0, (int) (t.f0 / maxValues.f0 * xBound) - 1),
										Math.max(0, (int) (t.f1 / maxValues.f1 * yBound) - 1)));
							});
						}
					})
					.reduceGroup(new GroupReduceFunction<Tuple2<Integer, Integer>, Integer[][]>() {
						@Override
						public void reduce(Iterable<Tuple2<Integer, Integer>> iterable, Collector<Integer[][]> collector) {
							final Integer[][] array = new Integer[xBound][yBound];
							IntStream.range(0, xBound)
									.parallel()
									.forEach(i -> Arrays.fill(array[i], 0));
							
							Iterator<Tuple2<Integer, Integer>> iterator = iterable.iterator();
							
							StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
									Spliterator.NONNULL | Spliterator.IMMUTABLE), true)
									.forEach(t -> {
										synchronized (array) {
											array[t.f0][t.f1]++;
										}
									});
							collector.collect(array);
						}
					})
					.collect();
			
			int[][] scatter = new int[xBound][yBound];
			Integer[][] scatterArray = plotPoints.get(0);
			IntStream.range(0, scatterArray.length)
					.parallel()
					.forEach(i -> scatter[i] = ArrayUtils.toPrimitive(scatterArray[i], 0));
			return scatter;
		} catch (Exception e) {
			throw new BusinessException(ExceptionUtils.getMessage(e), e);
		} finally {
			timer.stop();
			log.info("elapsed time for flink: " + timer.getTime() + "ms");
		}
	}
	
	
	private long createFilterMask(int... columnIndices) {
		long bitMask = 0;
		for (int columnIndex : columnIndices) {
			bitMask |= (1 << (columnIndex));
		}
		return bitMask;
	}
}