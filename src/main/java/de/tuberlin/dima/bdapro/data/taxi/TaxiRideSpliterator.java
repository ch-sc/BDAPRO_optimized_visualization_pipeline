package de.tuberlin.dima.bdapro.data.taxi;

import java.util.Spliterator;
import java.util.function.Consumer;

import de.tuberlin.dima.bdapro.data.GenericDataAccessor;
import lombok.Getter;

public class TaxiRideSpliterator implements Spliterator<TaxiRide> {
	
	
	private final GenericDataAccessor dataAccessor;
	private final int MIN_WORKLOAD_THRESHOLD = 5000;
	@Getter
	private final TaxiRide taxiRide;
	private int cursorOffset = 0;
	
	
	public TaxiRideSpliterator(GenericDataAccessor dataAccessor) {
		this.dataAccessor = dataAccessor;
		this.taxiRide = null;
	}
	
	
	TaxiRideSpliterator(TaxiRide taxiRide, GenericDataAccessor dataAccessor) {
		this.dataAccessor = dataAccessor;
		this.taxiRide = taxiRide;
		this.cursorOffset = taxiRide.getOffset();
	}
	
	
	@Override
	public boolean tryAdvance(Consumer<? super TaxiRide> action) {
		if (taxiRide.hasNext()) {
			taxiRide.next();
			action.accept(this.getTaxiRide());
			return true;
		} else {
			return false;
		}
	}
	
//	private Spliterator<TaxiRide> tryToSplit(){
//		if (!dataAccessor.hasNext()) {
//			return null;
//		}
//
//		int middle = dataAccessor.getCursor() + 1  + taxiRide.getLastCursorIndex();
//		if (middle - (dataAccessor.getCursor() + 1) < MIN_WORKLOAD_THRESHOLD) {
//			return null;
//		}
//
//		TaxiRide taxiRide = new TaxiRide(dataAccessor, taxi)
//
//		TaxiRideSpliterator partialSpliterator = new TaxiRideSpliterator(, dataAccessor)
//	}
	
	
	@Override
	public Spliterator<TaxiRide> trySplit() {
		if (!dataAccessor.hasNext()) {
			return null;
		}
		
		int skipped = Math.max(0, dataAccessor.getCursor());
		int rowsToProcess = dataAccessor.getLength() - skipped;
		int remainingRows = rowsToProcess - cursorOffset;
		if (remainingRows <= 0) {
			return null;
		} else if (remainingRows < MIN_WORKLOAD_THRESHOLD) {
			TaxiRide taxiRide = new TaxiRide(dataAccessor, cursorOffset, dataAccessor.getLength());
			cursorOffset = dataAccessor.getLength();
			return new TaxiRideSpliterator(taxiRide, dataAccessor);
		}
		
		int cores = Runtime.getRuntime().availableProcessors();
		int batchSize = Math.floorDiv(rowsToProcess, cores);
		int rest = rowsToProcess % cores;
		int partialLength = batchSize + rest;
		
		TaxiRide taxiRide = new TaxiRide(dataAccessor, cursorOffset, partialLength);
		
		cursorOffset += partialLength;
		
		return new TaxiRideSpliterator(taxiRide, dataAccessor);
	}
	
	
	@Override
	public long estimateSize() {
		return dataAccessor.getLength();
	}
	
	
	@Override
	public long getExactSizeIfKnown() {
		return dataAccessor.getLength();
	}
	
	
	
	
	
	@Override
	public int characteristics() {
		return Spliterator.NONNULL |
				Spliterator.IMMUTABLE |
				Spliterator.SIZED |
				Spliterator.CONCURRENT |
//				Spliterator.SUBSIZED |
				Spliterator.ORDERED;
	}
	
}
