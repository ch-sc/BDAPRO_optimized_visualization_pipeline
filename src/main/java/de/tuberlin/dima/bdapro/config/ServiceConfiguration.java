package de.tuberlin.dima.bdapro.config;

import java.io.File;

import de.tuberlin.dima.bdapro.data.DataProcessor;
import de.tuberlin.dima.bdapro.data.GenericDataAccessor;
import de.tuberlin.dima.bdapro.data.taxi.FlinkDataProcessor;
import de.tuberlin.dima.bdapro.data.taxi.ParallelDataProcessor;
import de.tuberlin.dima.bdapro.data.taxi.SimpleDataProcessor;
import de.tuberlin.dima.bdapro.data.taxi.TaxiRide;
import de.tuberlin.dima.bdapro.model.ExecutionType;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableConfigurationProperties(ServiceProperties.class)
@Configuration
public class ServiceConfiguration {
	
	
	@Autowired
	private ServiceProperties properties;
	
	
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
	
	
	public static DataProcessor dataProcessor(ExecutionType executionType, DataConfig config) {
		if (executionType == null) {
			executionType = ExecutionType.SEQUENTIAL;
		}
		
		DataProcessor dataProcessor;
		switch (executionType) {
			case SEQUENTIAL:
				dataProcessor = new SimpleDataProcessor(config, loadTaxiData(config));
				break;
			case PARALLEL:
				dataProcessor = new ParallelDataProcessor(loadTaxiData(config));
				break;
			case FLINK:
			default:
				// ToDo: set up Flink exec env properly
				dataProcessor = new FlinkDataProcessor(config, ExecutionEnvironment.getExecutionEnvironment());
				break;
		}
		
		return dataProcessor;
	}
	
	
	private static TaxiRide loadTaxiData(DataConfig config) {
		// ToDo: switch to a more generic approach
		// ToDo: load data from different data sources -> use JClouds or Hadoop
		GenericDataAccessor<TaxiRide> dataAccessor = new GenericDataAccessor<>(new File(config.getDataLocation()));
		dataAccessor.loadData(TaxiRide::loadData);
		return new TaxiRide(dataAccessor, dataAccessor.getCursor() + 0, dataAccessor.getLength());
	}
	
	
}
