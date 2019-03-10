package de.tuberlin.dima.bdapro.data;

import java.time.LocalDateTime;

import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.Point;
import lombok.AllArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

@AllArgsConstructor
public abstract class StreamProcessor {

    protected final StreamExecutionEnvironment env;

    public void run() throws Exception {
        this.env.execute();
    }

    public abstract DataStream<Tuple3<LocalDateTime,Point, ClusterCenter>> cluster(int xBound, int yBound, int k, int maxIter, Time window, Time slide);

    public abstract DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> scatterPlot(int x, int y, Time window, Time slide);

}
