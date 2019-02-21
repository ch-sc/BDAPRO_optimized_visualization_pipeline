package de.tuberlin.dima.bdapro.data.taxi;

import java.io.OutputStream;

import de.tuberlin.dima.bdapro.error.BusinessException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamDataProcessor {
	
	private final StreamExecutionEnvironment env;
	
	
	public StreamDataProcessor(StreamExecutionEnvironment env) {
		this.env = env;
	}
	
	
	public void streamedScatterPlot(int x, int y, OutputStream outputStream) {
		// execute the program
		try {
			
			env.execute("Streaming Iteration Example");
		} catch (Exception e) {
			throw new BusinessException(e.getMessage(), e);
		}
	}
	
	
	public void streamedScatterPlot(OutputStream outputStream) {
	
	}
	
}
