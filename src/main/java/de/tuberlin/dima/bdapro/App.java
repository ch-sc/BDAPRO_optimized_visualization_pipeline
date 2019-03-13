package de.tuberlin.dima.bdapro;

import java.time.LocalDateTime;
import java.util.Arrays;

import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.data.taxi.KMeansVDDA;
import de.tuberlin.dima.bdapro.data.taxi.StreamDataProcessor;
import de.tuberlin.dima.bdapro.data.taxi.VDDAProcessor;
import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.Point;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

@Slf4j
public class App {
	
	public static void main(String[] args) {
		log.info("App args: " + Arrays.toString(args));
		/*
		DataConfig config;
		try {
			config = AppConfigLoader.load(null);
		} catch (Throwable e) {
//			e.printStackTrace();
			log.error("Could not start application: " + ExceptionUtils.getMessage(e), e);
			return;
		}*/
		/*
		ExecutionType executionType = ExecutionType.SEQUENTIAL;
		
		if (args.length > 0) {
			String arg1 = args[0];
			if (arg1.equalsIgnoreCase("flink") || arg1.equalsIgnoreCase("streamed")) {
				executionType = ExecutionType.FLINK;
			} else if (arg1.equalsIgnoreCase("sequential") || arg1.equalsIgnoreCase("seq")) {
				executionType = ExecutionType.SEQUENTIAL;
			} else if (arg1.equalsIgnoreCase("parallel")) {
				executionType = ExecutionType.PARALLEL;
			} else if (arg1.equalsIgnoreCase("simpleStreaming")) {
				executionType = ExecutionType.SIMPLESTREAMING;
			}else {
				throw new IllegalArgumentException("first parameter specifies execution type. Can be " + Arrays
						.toString(ExecutionType.values()));
			}
		}*/
		
		//final DataProcessor dataProcessor = ServiceConfiguration.dataProcessor(ExecutionType.SIMPLESTREAMING, null);


		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		final StreamProcessor streamProcessor1 = new StreamDataProcessor(env);
		final StreamProcessor streamProcessor2 = new VDDAProcessor(env);
		final StreamProcessor cluster = new KMeansVDDA(env);
		
		int x = 320, y = 480;
		/*
		int[][] scatter;
		switch (executionType) {
			case SEQUENTIAL:
				scatter = dataProcessor.scatterPlot(x, y);
				break;
			case PARALLEL:
				scatter = dataProcessor.scatterPlot(x, y);
				break;
			case FLINK:
			default:
				scatter = dataProcessor.scatterPlot(x, y);
				break;
		}
		
		logResult(scatter, x, y);*/

		//DataStream<Point> points = streamProcessor1.scatterPlot(x,y);

		//DataStream<Tuple2<Point, ClusterCenter>> clusters = streamProcessor1.cluster(5, 5, points);

		StopWatch timer = new StopWatch();
		timer.start();

		DataStream<Tuple3<LocalDateTime, Point, ClusterCenter>> clusters = cluster.cluster(x,y,5,5, Time.hours(20), Time.hours(0));

		timer.stop();
		log.info("Time for total " + timer.getTime() + "ms");

	}
	
	
	private static void logResult(int[][] scatter, int x, int y) {
		StringBuilder stringBuffer = new StringBuilder(100 * 100 * 2).append("Output:\n");
		for (int i = 0; i < Math.min(100, x - 1); i++) {
			for (int j = 0; j < Math.min(100, y - 1); j++) {
				stringBuffer.append(scatter[i][j]).append(" ");
			}
			stringBuffer.append("\n");
		}
		log.info(stringBuffer.toString());
	}
	
}
