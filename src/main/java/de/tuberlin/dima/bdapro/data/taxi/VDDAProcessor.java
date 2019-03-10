package de.tuberlin.dima.bdapro.data.taxi;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.Point;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Applies VDDA transformation to data stream
 */
@Slf4j
public class VDDAProcessor extends StreamProcessor {


    private final StreamExecutionEnvironment env;


    public VDDAProcessor(StreamExecutionEnvironment env) {
        this.env = env;
    }

    @Override
    public DataStream<Tuple3<LocalDateTime, Point, ClusterCenter>> cluster(int xBound, int yBound, int k, int maxIter, Time window, Time slide) {
        return null;
    }

    /**
     *
     * Calculates the VDDA transformation for the inputed data
     *
     * @param x Number of Pixels in the x direction
     * @param y Number of Pixels in the y direction
     * @return data stream with a timestamp, a key for the display bucket, the coordinates of the point, and the number of points inside one bucket
     */

    @Override
    public DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> scatterPlot(int x, int y, Time window, Time slide) {

        StopWatch timer = new StopWatch();
        timer.start();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> dataStream;
        // execute the program

        //read input file
        dataStream = env.readTextFile("data/yellow_tripdata_2017-12.csv");

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
        DataStream<Tuple3<LocalDateTime, Double, Double>> pDataStream = dataStream.flatMap(new FlatMapFunction<String, Tuple3<LocalDateTime, Double, Double>>() {
            @Override
            public void flatMap(String s, Collector<Tuple3<LocalDateTime, Double, Double>> out) throws Exception {
                if (!s.equalsIgnoreCase(null)) {
                    String[] helper = s.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                    if (helper.length >= 10) {
                        String helper2 = helper[1];
                        String[] helper3 = helper2.split(" ");
                        helper2 = helper3[0] + "T" + helper3[1];
                        LocalDateTime dateTime = LocalDateTime.parse(helper2);
                        Tuple3<LocalDateTime, Double, Double> output = new Tuple3<>(dateTime, Double.valueOf(helper[7]), Double.valueOf(helper[8]));
                        out.collect(output);
                    }
                }
            }
        });

        DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> resultStream = pDataStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<LocalDateTime, Double, Double>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<LocalDateTime, Double, Double> event) {
                        return Long.valueOf(event.f0.getSecond() + event.f0.getMinute() + event.f0.getHour() + event.f0.getDayOfYear() + event.f0.getYear());
                    }
                }).keyBy(0).window(SlidingEventTimeWindows.of(window, slide))
                .apply(new WindowFunction<Tuple3<LocalDateTime, Double, Double>, Tuple4<LocalDateTime, Double, Point, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<LocalDateTime, Double, Double>> iterable, Collector<Tuple4<LocalDateTime, Double, Point, Integer>> collector) throws Exception {


                        double xMax = Double.MIN_VALUE;
                        double xMin = Double.MAX_VALUE;
                        double yMax = Double.MIN_VALUE;
                        double yMin = Double.MAX_VALUE;
                        double yVal = 0;
                        double xVal = 0;
                        Map<Double, Tuple3<LocalDateTime, Point, Integer>> valueMap = new HashMap<>();

                        for (Tuple3<LocalDateTime, Double, Double> v : iterable) {
                            if (xMax <= v.f1) {
                                xMax = v.f1;
                            }
                            if (xMin >= v.f1) {
                                xMin = v.f1;
                            }
                            if (yMax <= v.f2) {
                                yMax = v.f2;
                            }
                            if (yMin >= v.f2) {
                                yMin = v.f2;
                            }
                        }

                        double ydiv = yMax - yMin;
                        double xdiv = xMax - xMin;

                        for (Tuple3<LocalDateTime, Double, Double> v : iterable) {
                            if (ydiv != 0) {
                                yVal = Math.round(y * (v.f2 - yMin) / ydiv);
                            } else {
                                yVal = Math.round(y * (v.f2 - yMin) / 1);
                            }
                            if (xdiv != 0) {
                                xVal = Math.round(x * (v.f1 - xMin) / xdiv);
                            } else {
                                xVal = Math.round(x * (v.f1 - xMin) / 1);

                            }

                            double key = x * yVal + xVal;
                            double[] data = new double[]{v.f1, v.f2};

                            Point point = new Point(data);

                            //count number of occurences
                            valueMap.computeIfPresent(key, (k, value) -> new Tuple3<LocalDateTime, Point, Integer>(value.f0, value.f1, value.f2 + 1));
                            //only save the first incoming point per window
                            valueMap.putIfAbsent(key, new Tuple3<LocalDateTime, Point, Integer>(v.f0, point, 1));
                        }

                        valueMap.entrySet().stream()
                                .forEach(e -> {
                                    collector.collect(new Tuple4<LocalDateTime, Double, Point, Integer>(e.getValue().f0, e.getKey(), e.getValue().f1, e.getValue().f2));
                                });

                    }
                });

        timer.stop();
        log.info("Time for VDDA " + timer.getTime() + "ms");

        return resultStream;
    }




}
