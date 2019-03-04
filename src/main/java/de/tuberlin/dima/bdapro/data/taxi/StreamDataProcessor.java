package de.tuberlin.dima.bdapro.data.taxi;

import java.util.*;
import java.io.OutputStream;

import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.error.BusinessException;
import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.Point;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Int;

/**
 * Applies M4 transformation to data stream
 */

public class StreamDataProcessor extends StreamProcessor {

    private final StreamExecutionEnvironment env;


    public StreamDataProcessor(StreamExecutionEnvironment env) {
        this.env = env;
    }


    @Override
    public DataStream<Point> scatterPlot(int x, int y) {

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> dataStream;
        // execute the program

        //read input file
        dataStream = env.readTextFile("/home/eleicha/Repos/BDAPRO_neu/BDAPRO_optimized_visualization_pipeline/data/9000000.csv");

        //filter for the first two rows
        dataStream = dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                if (!s.equalsIgnoreCase(null) && !s.contains("VendorID")) {
                    return true;
                }
                return false;
            }
        });


        //get desired data fields from input file
        DataStream<Tuple2<Integer, Integer>> pDataStream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                if (!s.equalsIgnoreCase(null)) {
                    String[] helper = s.split(",");
                    if (helper.length >= 10) {
                        Tuple2<Integer, Integer> output = new Tuple2<Integer, Integer>(100*Double.valueOf(helper[4]).intValue(), 100*Double.valueOf(helper[10]).intValue());
                        out.collect(output);
                    }
                }
            }
        });

        //apply custom VDDA function
        DataStream<Tuple3<Integer, Integer, Long>> window = pDataStream.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple2<Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Integer, Integer> element) {
                        return System.currentTimeMillis();
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new AllWindowFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Long>, TimeWindow>() {


                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple2<Integer, Integer>> iterable, Collector<Tuple3<Integer, Integer, Long>> collector) throws Exception {

                        int xMax = Integer.MIN_VALUE;
                        int xMin = Integer.MAX_VALUE;
                        int yMax = Integer.MIN_VALUE;
                        int yMin = Integer.MAX_VALUE;
                        int yVal = 0;
                        int xVal = 0;
                        Map<Long, Long> valueMap = new HashMap<>();

                        for (Tuple2<Integer, Integer> v : iterable) {
                            if (xMax <= v.f0) {
                                xMax = v.f0;
                            }
                            if (xMin >= v.f0) {
                                xMin = v.f0;
                            }
                            if (yMax <= v.f1) {
                                yMax = v.f1;
                            }
                            if (yMin >= v.f1) {
                                yMin = v.f1;
                            }
                        }

                        for (Tuple2<Integer, Integer> v : iterable) {
                            yVal = Math.round(y * (v.f1 - yMin) / (yMax - yMin));
                            xVal = Math.round(x * (v.f0 - xMin) / (xMax - xMin));

                            long key = ((long)xVal) << 32 | yVal;

                            valueMap.putIfAbsent(key, 1L);
                            valueMap.computeIfPresent(key, (k, value) -> value+1);

                            // if (valueMap.isEmpty() || !valueMap.contains(new Tuple2<>(xVal, yVal))) {
                            //     valueMap.add(new Tuple2<>(xVal, yVal));
                            //     collector.collect(new Tuple2<>(xVal, yVal));
                            // }
                        }

                        valueMap.entrySet().stream()
                                .forEach(e -> {
                                    int xKey = (int)(e.getKey() >>32);
                                    int yKey = e.getKey().intValue();
                                    collector.collect(new Tuple3<Integer, Integer, Long>(xKey, yKey, e.getValue()));
                                });

                    }
                });

        DataStream<Point> points = window.map(new MapFunction<Tuple3<Integer, Integer, Long>, Point>() {
            @Override
            public Point map(Tuple3<Integer, Integer, Long> input) throws Exception {

                double[] data = new double[]{input.f0, input.f1};

                Point point = new Point(data);

                return point;
            }
        });

        try {
            env.execute("Streaming Iteration Example");
        } catch (Exception e) {
            throw new BusinessException(e.getMessage(), e);
        }

        return points;
    }

    @Override
    public DataStream<Tuple2<Point, ClusterCenter>> cluster(int xBound, int yBound, int k, int maxIter) {
        return null;
    }



}
