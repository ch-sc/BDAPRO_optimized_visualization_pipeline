package de.tuberlin.dima.bdapro.data;

import java.io.OutputStream;

public abstract class StreamedDataProcessor extends DataProcessor {
	
	public abstract void streamedScatterPlot(int x, int y, OutputStream outputStream);
	
	public abstract void streamedScatterPlot(OutputStream outputStream);
	
}
