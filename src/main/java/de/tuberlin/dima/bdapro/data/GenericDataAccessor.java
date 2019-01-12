package de.tuberlin.dima.bdapro.data;

import java.io.File;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import lombok.Getter;


public class GenericDataAccessor<T> implements IDataAccessor {
	
	@Getter
	protected int cursor = -1;
	@Getter
	protected int length = 0;
	@Getter
	private DataStore dataStore;
	
	@Getter
	private File file;
	
	
	protected GenericDataAccessor() {
	
	}
	
	
	public GenericDataAccessor(File file) {
		this.file = file;
	}
	
	
	public void loadData(BiFunction<File, GenericDataAccessor, Integer> loadDataFunction) {
		clear();
		dataStore = new DataStore();
		System.gc();
		this.length = loadDataFunction.apply(this.file, this);
	}
	
	
	public void reset() {
		cursor = -1;
	}
	
	
	private void clear() {
		cursor = -1;
		length = 0;
		dataStore = null;
	}
	
	
	public boolean hasNext() {
		return cursor < length - 1;
	}
	
	
	public synchronized boolean next() {
		cursor++;
		return hasNext();
	}
	
	
	public Stream<T> stream(Streamable<T> streamable) {
		reset();
		
		if (!hasNext()) {
			return Stream.empty();
		}


//		return Stream
//				.generate(() -> {
//					next();
//					return new TaxiRide(this, cursor);
//				})
////				.parallel()
//				.limit(length);
		
		next();
		return streamable.stream();
		
//		TaxiRide taxiRide = new TaxiRide(this, cursor, length);
//		return StreamSupport.stream(new TaxiRide.TaxiRideSpliterator(this), true);
//		next();
//		return Stream
//				.iterate(new TaxiRide(this, cursor), taxiRide -> {
//					this.next();
//					taxiRide.setPartialCursor(cursor);
//					return taxiRide;
////					return new TaxiRide(this, cursor);
//				})
//				.limit(length);
	}
}
