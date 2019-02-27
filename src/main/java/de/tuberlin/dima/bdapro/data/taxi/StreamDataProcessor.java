package de.tuberlin.dima.bdapro.data.taxi;

import java.util.*;
import java.io.OutputStream;

import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.error.BusinessException;
import de.tuberlin.dima.bdapro.model.Point;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Applies M4 transformation to data stream
 */

public class StreamDataProcessor extends StreamProcessor {
	
	private final StreamExecutionEnvironment env;
	
	
	public StreamDataProcessor(StreamExecutionEnvironment env) {
		this.env = env;
	}


	@Override
	public DataStream<Point> scatterPlot(int x, int y) {

		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<String> dataStream;
		// execute the program

			//read input file
		dataStream = env.readTextFile("/home/eleicha/Repos/BDAPRO_neu/BDAPRO_optimized_visualization_pipeline/data/1000000.csv");

			//filter for the first two rows
			dataStream = dataStream.filter(new FilterFunction<String>() {
				@Override
				public boolean filter(String s) throws Exception {
					if (!s.equalsIgnoreCase(null) && !s.contains("VendorID")){
						return true;
					}
					return false;
				}
			});


			//get desired data fields from input file
			DataStream<Tuple2<Double,Double>> pDataStream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<Double, Double>>() {
				@Override
				public void flatMap(String s, Collector<Tuple2<Double,Double>> out) throws Exception {
					if (!s.equalsIgnoreCase(null)){
						String[] helper = s.split(",");
						if (helper.length >= 10) {
							Tuple2<Double, Double> output = new Tuple2<Double, Double>(Double.parseDouble(helper[4]), Double.parseDouble(helper[10]));
							out.collect(output);
						}
					}
				}
			});

			//apply custom M4 function
			DataStream<Tuple2<Double,Double>> window = pDataStream
					.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Double, Double>>() {
				@Override
				public long extractAscendingTimestamp(Tuple2<Double, Double> element) {
					return System.currentTimeMillis();
				}
			})
					.windowAll(TumblingEventTimeWindows.of(Time.seconds(60)))
					.apply(new AllWindowFunction<Tuple2<Double, Double>, Tuple2<Double,Double>, TimeWindow>() {


						@Override
						public void apply(TimeWindow timeWindow, Iterable<Tuple2<Double, Double>> iterable, Collector<Tuple2<Double, Double>> collector) throws Exception {

							double xMax = Double.MIN_VALUE;
							double xMin = Double.MAX_VALUE;
							double yMax = Double.MIN_VALUE;
							double yMin = Double.MAX_VALUE;
							double yVal = 0;
							double xVal = 0;
							List<Tuple2<Double,Double>> vals = new ArrayList<>();
							double finalVal = 0;

							for (Tuple2<Double,Double> v: iterable){
								if (xMax <= v.f0){
									xMax = v.f0;
								}
								if (xMin >= v.f0){
									xMin = v.f0;
								}
								if (yMax <= v.f1){
									yMax = v.f1;
								}
								if (yMin >= v.f1){
									yMin = v.f1;
								}
							}

							for (Tuple2<Double, Double> v: iterable){
								yVal = Math.round(y * (v.f1 - yMin)/(yMax-yMin));
								xVal = Math.round(x * (v.f0 - xMin)/(xMax-xMin));
								if (vals.isEmpty() || !vals.contains(new Tuple2<>(xVal,yVal))){
									vals.add(new Tuple2<>(xVal,yVal));
									collector.collect(new Tuple2<>(xVal,yVal));
								}
							}


						}
					});

			DataStream<Point> points = window.map(new MapFunction<Tuple2<Double, Double>, Point>() {
				@Override
				public Point map(Tuple2<Double, Double> input) throws Exception {

					double[] data = new double[]{input.f0, input.f1};

					Point point = new Point(data);

					return point;
				}
			});


		try {
			env.execute("Streaming Iteration Example");
		} catch (Exception e) {
			throw new BusinessException(e.getMessage(), e);
		}

		return points;
	}


	public void streamedScatterPlot(OutputStream outputStream) {
	
	}


}
