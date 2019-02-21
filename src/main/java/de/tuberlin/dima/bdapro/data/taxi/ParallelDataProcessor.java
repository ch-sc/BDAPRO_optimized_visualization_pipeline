package de.tuberlin.dima.bdapro.data.taxi;

import de.tuberlin.dima.bdapro.data.GenericDataAccessor;
import de.tuberlin.dima.bdapro.data.DataProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.java.tuple.Tuple2;

@Slf4j
public class ParallelDataProcessor extends DataProcessor {
	
	private TaxiRide taxiRide;
	
	
	public ParallelDataProcessor(TaxiRide taxiRide) {
		this.taxiRide = taxiRide;
	}
	
	@Override
	public int[][] scatterPlot(int xBound, int yBound) {
		StopWatch timer = new StopWatch();
		timer.start();
		int maxDistance;
		int maxFare;
		
		Tuple2<Integer, Integer> maxValuesTuple = taxiRide.stream()
				.reduce(new Tuple2<>(0, 0),
						(tuple, taxiRide) -> {
							if (taxiRide.getDistance() > tuple.f0) {
								tuple.f0 = taxiRide.getDistance();
							}
							if (taxiRide.getFare() > tuple.f1) {
								tuple.f1 = taxiRide.getFare();
							}
							return tuple;
						},
						(tuple1, tuple2) -> {
							if (tuple1.f0 < tuple2.f0) {
								tuple1.f0 = tuple2.f0;
							}
							if (tuple1.f1 < tuple2.f1) {
								tuple1.f1 = tuple2.f1;
							}
							return tuple1;
						});
		
		maxDistance = maxValuesTuple.f0;
		maxFare = maxValuesTuple.f1;
		
		int[][] scatterPlot = new int[xBound][yBound];
		taxiRide.stream()
				.forEach(taxiRide -> scatterPlot
						[Math.max(0, (int) (((double) this.taxiRide.getDistance() / maxDistance) * xBound) -1)]
						[Math.max(0, (int) (((double) this.taxiRide.getFare() / maxFare) * yBound) - 1)]++
				);
		
		timer.stop();
		log.info("elapsed time for parallel streamed data: " + timer.getTime() + "ms");
		
		return scatterPlot;
	}
	
	
	@Override
	public int[][] scatterPlot() {
		return new int[0][];
	}
	
}
