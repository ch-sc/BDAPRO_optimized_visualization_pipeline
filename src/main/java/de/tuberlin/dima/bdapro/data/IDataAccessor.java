package de.tuberlin.dima.bdapro.data;

public interface IDataAccessor {
	boolean hasNext();
	boolean next();
	void reset();
}
