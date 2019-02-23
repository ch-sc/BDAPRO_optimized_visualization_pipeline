package de.tuberlin.dima.bdapro.data.taxi;

import java.io.OutputStream;

import de.tuberlin.dima.bdapro.error.BusinessException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamDataProcessor {
	
	private final StreamExecutionEnvironment env;
	
	
	public StreamDataProcessor(StreamExecutionEnvironment env) {
		this.env = env;
	}
	
	
	public void streamedScatterPlot(int x, int y, OutputStream outputStream) {



		DataStream<String> dataStream;
		// execute the program
		try {
			dataStream = env.readTextFile("csv", "/home/eleicha/Repos/BDAPRO_neu/BDAPRO_optimized_visualization_pipeline/data/1000000.csv");

			dataStream = dataStream.filter(new FilterFunction<String>() {
				@Override
				public boolean filter(String s) throws Exception {
					if (!s.equalsIgnoreCase(null) && !s.contains("VendorID")){
						return true;
					}
					return false;
				}
			});

			dataStream.map(new MapFunction<String, Tuple2<Double,Double>>() {
				@Override
				public Tuple2<Double, Double> map(String s) throws Exception {

						String[] subString = s.split(",");
						Tuple2<Double,Double> data = new Tuple2(subString[4], subString[10]);

					return data;
				}
			});

			

			env.execute("Streaming Iteration Example");
		} catch (Exception e) {
			throw new BusinessException(e.getMessage(), e);
		}
	}
	
	
	public void streamedScatterPlot(OutputStream outputStream) {
	
	}
	
}
