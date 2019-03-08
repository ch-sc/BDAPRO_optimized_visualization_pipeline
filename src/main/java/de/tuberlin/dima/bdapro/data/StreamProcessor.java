package de.tuberlin.dima.bdapro.data;

import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.Point;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.time.LocalDateTime;

public abstract class StreamProcessor {

    abstract public DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> scatterPlot(int xBound, int yBound);

    abstract public DataStream<Tuple3<LocalDateTime,Point, ClusterCenter>> cluster(int xBound, int yBound, int k, int maxIter);

    }
