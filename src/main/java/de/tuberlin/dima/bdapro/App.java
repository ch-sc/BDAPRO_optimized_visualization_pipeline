package de.tuberlin.dima.bdapro;

import java.util.Arrays;

import de.tuberlin.dima.bdapro.config.AppConfigLoader;
import de.tuberlin.dima.bdapro.config.DataConfig;
import de.tuberlin.dima.bdapro.config.ServiceConfiguration;
import de.tuberlin.dima.bdapro.data.DataProcessor;
import de.tuberlin.dima.bdapro.model.ExecutionType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Slf4j
public class App {
	
	public static void main(String[] args) {
		log.info("App args: " + Arrays.toString(args));
		
		DataConfig config;
		try {
			config = AppConfigLoader.load(null);
		} catch (Throwable e) {
//			e.printStackTrace();
			log.error("Could not start application: " + ExceptionUtils.getMessage(e), e);
			return;
		}
		
		ExecutionType executionType = ExecutionType.SEQUENTIAL;
		
		if (args.length > 0) {
			String arg1 = args[0];
			if (arg1.equalsIgnoreCase("flink") || arg1.equalsIgnoreCase("streamed")) {
				executionType = ExecutionType.FLINK;
			} else if (arg1.equalsIgnoreCase("sequential") || arg1.equalsIgnoreCase("seq")) {
				executionType = ExecutionType.SEQUENTIAL;
			} else if (arg1.equalsIgnoreCase("parallel")) {
				executionType = ExecutionType.PARALLEL;
			} else {
				throw new IllegalArgumentException("first parameter specifies execution type. Can be " + Arrays
						.toString(ExecutionType.values()));
			}
		}
		
		final DataProcessor dataProcessor = ServiceConfiguration.dataProcessor(executionType, config);
		int x = 1000, y = 1000;
		
		
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
	
}
