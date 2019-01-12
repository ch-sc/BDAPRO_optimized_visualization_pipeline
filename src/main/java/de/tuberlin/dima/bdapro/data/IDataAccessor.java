package de.tuberlin.dima.bdapro.data;

public interface IDataAccessor {
	public boolean hasNext();
	public boolean next();
	public void reset();
}
