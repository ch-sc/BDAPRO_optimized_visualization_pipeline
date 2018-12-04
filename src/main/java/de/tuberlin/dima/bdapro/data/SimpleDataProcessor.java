package de.tuberlin.dima.bdapro.data;

import de.tuberlin.dima.bdapro.config.AppConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

@Slf4j
public class SimpleDataProcessor extends DataProcessor {
	
	private AppConfig config;
	private DataAccessor dataAccessor;
	
	public SimpleDataProcessor(AppConfig config, DataAccessor dataAccessor) {
		this.config = config;
		this.dataAccessor = dataAccessor;
	}
	
	
	@Override
	public int[][] scatterPlot(int xBound, int yBound) {
		StopWatch timer = new StopWatch();
		timer.start();
		
		int maxDistance = 0;
		int maxFare = 0;
		
		while (dataAccessor.next()) {
			if (maxDistance < dataAccessor.getDistance())
				maxDistance = dataAccessor.getDistance();
			if (maxFare < dataAccessor.getFare())
				maxFare = dataAccessor.getFare();
		}

		dataAccessor.reset();

		int[][] scatterPlot = new int[xBound][yBound];
		while (dataAccessor.next()) {
			scatterPlot[Math.max(0, (int) (((double) dataAccessor.getDistance() / maxDistance) * xBound) -1)]
					[Math.max(0, (int) (((double) dataAccessor.getFare() / maxFare) * yBound) - 1)]++;
		}
		timer.stop();
		log.info("elapsed time for vanilla java: " + timer.getTime() + "ms");
//		System.out.println("elapsed time for vanilla java: " + timer.elapsed(TimeUnit.MILLISECONDS) + "ms");
		
		return scatterPlot;
	}
}
