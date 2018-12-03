package de.tuberlin.dima.bdapro;

import java.io.File;
import java.util.Arrays;

import de.tuberlin.dima.bdapro.config.AppConfig;
import de.tuberlin.dima.bdapro.config.AppConfigLoader;
import de.tuberlin.dima.bdapro.data.DataAccessor;
import de.tuberlin.dima.bdapro.data.DataProcessor;
import de.tuberlin.dima.bdapro.data.FlinkDataProcessor;
import de.tuberlin.dima.bdapro.data.VanillaJavaDataProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.java.ExecutionEnvironment;

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
			} else
				throw new IllegalArgumentException("first parameter specifies execution type. Can be " + Arrays
						.toString(ExecutionType.values()));
		}
		
		int x=1000, y=1000;
		
		int[][] scatter;
		switch (executionType){
			case NATIVE:
				
				dataProcessor = new VanillaJavaDataProcessor(config, loadData(config));
				scatter = dataProcessor.scatterPlot(x, y);
				break;
			case FLINK:
				dataProcessor = new FlinkDataProcessor(config, ExecutionEnvironment.getExecutionEnvironment());
				scatter = dataProcessor.scatterPlot(x, y);
				break;
		}
		
		
//		if (log.isInfoEnabled()){
//			Integer[][] subarray = ArrayUtils.subarray(scatter, 0, Math.min(100, scatter.length-1));
//			for (Integer[] dots : subarray) {
//				log.info(Arrays.deepToString(ArrayUtils.subarray(scatter, 0, Math.min(100, scatter.length-1))));
//			}
//		}
	}
	
	public static DataAccessor loadData(AppConfig config) {
		DataAccessor dataAccessor = new DataAccessor(new File(config.getDataLocation()));
		dataAccessor.loadData();
		return dataAccessor;
	}
	
	private static enum ExecutionType {
		NATIVE,
		FLINK
	}
	
	
}
