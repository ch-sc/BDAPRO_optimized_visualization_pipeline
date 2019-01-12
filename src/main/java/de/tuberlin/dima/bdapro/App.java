package de.tuberlin.dima.bdapro;

import java.io.File;
import java.util.Arrays;

import de.tuberlin.dima.bdapro.config.AppConfig;
import de.tuberlin.dima.bdapro.config.AppConfigLoader;
import de.tuberlin.dima.bdapro.data.GenericDataAccessor;
import de.tuberlin.dima.bdapro.data.DataProcessor;
import de.tuberlin.dima.bdapro.data.taxi.FlinkDataProcessor;
import de.tuberlin.dima.bdapro.data.taxi.ParallelDataProcessor;
import de.tuberlin.dima.bdapro.data.taxi.SimpleDataProcessor;
import de.tuberlin.dima.bdapro.data.taxi.TaxiRide;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

@Slf4j
public class App {
	
	public static void main(String[] args) {
		log.info("App args: " + Arrays.toString(args));
		
		DataProcessor dataProcessor;
		AppConfig config;
		try {
			config = AppConfigLoader.load(null);
		} catch (Throwable e) {
//			e.printStackTrace();
			log.error("Could not start application: " + ExceptionUtils.getMessage(e), e);
			return;
		}
		
		ExecutionType executionType = ExecutionType.NATIVE;
		
		if (args.length > 0) {
			String arg1 = args[0];
			if (arg1.equalsIgnoreCase("flink")) {
				executionType = ExecutionType.FLINK;
			} else if (arg1.equalsIgnoreCase("native")) {
				executionType = ExecutionType.NATIVE;
			} else if (arg1.equalsIgnoreCase("parallel")) {
				executionType = ExecutionType.NATIVE_PARALLEL;
			} else {
				throw new IllegalArgumentException("first parameter specifies execution type. Can be " + Arrays
						.toString(ExecutionType.values()));
			}
		}
		
		int x = 1000, y = 1000;
		
		int[][] scatter;
		switch (executionType) {
			case NATIVE:
				
				dataProcessor = new SimpleDataProcessor(config, loadTaxiData(config));
				scatter = dataProcessor.scatterPlot(x, y);
				break;
			case NATIVE_PARALLEL:
				dataProcessor = new ParallelDataProcessor(loadTaxiData(config));
				scatter = dataProcessor.scatterPlot(x, y);
				break;
			case FLINK:
			default:
				dataProcessor = new FlinkDataProcessor(config, ExecutionEnvironment.getExecutionEnvironment());
				scatter = dataProcessor.scatterPlot(x, y);
				break;
		}
		
		logResult(scatter, x, y);
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
	
	
	public static TaxiRide loadTaxiData(AppConfig config) {
		GenericDataAccessor<TaxiRide> dataAccessor = new GenericDataAccessor<>(new File(config.getDataLocation()));
		dataAccessor.loadData(TaxiRide::loadData);
		return new TaxiRide(dataAccessor, dataAccessor.getCursor() + 0, dataAccessor.getLength());
	}
	
	
	private enum ExecutionType {
		NATIVE,
		NATIVE_PARALLEL,
		FLINK
	}
	
}
