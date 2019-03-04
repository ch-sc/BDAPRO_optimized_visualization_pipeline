package de.tuberlin.dima.bdapro.data;

import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.Point;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

public abstract class StreamProcessor {

    abstract public DataStream<Point> scatterPlot(int xBound, int yBound);

    abstract public DataStream<Tuple2<Point, ClusterCenter>> cluster(int xBound, int yBound, int k, int maxIter);



    }
