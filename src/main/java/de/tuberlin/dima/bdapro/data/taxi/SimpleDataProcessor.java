package de.tuberlin.dima.bdapro.data.taxi;

import de.tuberlin.dima.bdapro.config.DataConfig;
import de.tuberlin.dima.bdapro.data.DataProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;

@Slf4j
public class SimpleDataProcessor extends DataProcessor {
	
	private DataConfig config;
	private TaxiRide taxiRide;
	
	
	public SimpleDataProcessor(DataConfig config, TaxiRide taxiRide) {
		this.config = config;
		this.taxiRide = taxiRide;
	}
	
	
	@Override
	public int[][] scatterPlot(int xBound, int yBound) {
		StopWatch timer = new StopWatch();
		timer.start();
		
		int maxDistance = 0;
		int maxFare = 0;
		
		taxiRide.reset();
		
		while (taxiRide.next()) {
			if (maxDistance < taxiRide.getDistance()) {
				maxDistance = taxiRide.getDistance();
			}
			if (maxFare < taxiRide.getFare()) {
				maxFare = taxiRide.getFare();
			}
		}
		
		taxiRide.reset();
		
		int[][] scatterPlot = new int[xBound][yBound];
		while (taxiRide.next()) {
			scatterPlot[Math.max(0, (int) (((double) taxiRide.getDistance() / maxDistance) * xBound) - 1)]
					[Math.max(0, (int) (((double) taxiRide.getFare() / maxFare) * yBound) - 1)]++;
		}
		timer.stop();
		log.info("elapsed time for vanilla java: " + timer.getTime() + "ms");
//		System.out.println("elapsed time for vanilla java: " + timer.elapsed(TimeUnit.MILLISECONDS) + "ms");
		
		return scatterPlot;
	}
	
	
	@Override
	public int[][] scatterPlot() {
		StopWatch timer = new StopWatch();
		timer.start();
		
		taxiRide.reset();
//		new IntStream().
//		final Stream<Integer> stream =
//				StreamSupport.stream(new Spliterators.AbstractSpliterator<TaxiRide>(taxiRide.getLength(), Spliterator.NONNULL |
//						Spliterator.IMMUTABLE | Spliterator.SIZED | Spliterator.ORDERED) {
//					@Override
//					public boolean tryAdvance(IntConsumer action) {
//						return taxiRide.hasNext();
//					}
//
//
//					@Override
//					public void forEachRemaining(IntConsumer action) {
//
//					}
//				}, false);
		
		if (!taxiRide.hasNext()) {
			return new int[0][];
		}
		
		taxiRide.next();
		int maxDistance = taxiRide.getDistance(), minDistance = taxiRide.getDistance(),
				maxFare = taxiRide.getFare(), minFare = taxiRide.getFare();
		
		while (taxiRide.next()) {
			if (maxDistance < taxiRide.getDistance()) {
				maxDistance = taxiRide.getDistance();
			} else if (minDistance > taxiRide.getDistance()) {
				minDistance = taxiRide.getDistance();
			}
			if (maxFare < taxiRide.getFare()) {
				maxFare = taxiRide.getFare();
			} else if (minDistance > taxiRide.getFare()) {
				minFare = taxiRide.getFare();
			}
		}
		
		taxiRide.reset();
		
		int xDim = Math.abs(maxDistance - minDistance),
				yDim = Math.abs(maxFare - minFare);
		
		int[][] scatterPlot = new int[xDim + 1][yDim + 1];
		
		while (taxiRide.next()) {
			scatterPlot[(int) ((double) taxiRide.getDistance() - minDistance)]
					[(int) ((double) taxiRide.getFare() - minFare)]++;
		}
		
		timer.stop();
		log.info("elapsed time for vanilla java: " + timer.getTime() + "ms");
		
		return scatterPlot;
	}
}
