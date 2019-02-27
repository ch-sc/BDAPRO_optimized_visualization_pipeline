package de.tuberlin.dima.bdapro.data;

import de.tuberlin.dima.bdapro.model.Point;
import org.apache.flink.streaming.api.datastream.DataStream;

public abstract class StreamProcessor {

    abstract public DataStream<Point> scatterPlot(int xBound, int yBound);


}
