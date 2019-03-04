package de.tuberlin.dima.bdapro.data.taxi;

import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.error.BusinessException;
import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.Point;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.commons.lang3.time.StopWatch;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class KMeansVDDA extends StreamProcessor{

    private final StreamExecutionEnvironment env;


    public KMeansVDDA(StreamExecutionEnvironment env) {
        this.env = env;
    }

    @Override
    public DataStream<Tuple2<Point, ClusterCenter>> cluster(int xBound, int yBound, int k, int maxIter) {

        StopWatch timer = new StopWatch();
        timer.start();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Point> dataPoints = scatterPlot(xBound, yBound);

        //initialize the first points as cluster centers
        DataStream<Tuple2<Point, ClusterCenter>> clusters = dataPoints
                .windowAll(SlidingProcessingTimeWindows.of(Time.milliseconds(40), Time.milliseconds(10)))
                .apply(new AllWindowFunction<Point, Tuple2<Point, ClusterCenter>, TimeWindow>() {


                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Point> iterable, Collector<Tuple2<Point, ClusterCenter>> collector) throws Exception {

                        int counter = 0;
                        List<ClusterCenter> clusterCenters = new ArrayList<>();

                        //take the first k points as initial cluster centers
                        for (Point p : iterable) {
                            if (counter < k) {

                                clusterCenters.add(new ClusterCenter(counter, p));
                                counter++;

                            }
                        }

                        counter = 0;
                        Tuple2<Point, ClusterCenter> helper = new Tuple2<>();
                        List<Tuple2<Point, ClusterCenter>> finalList = new ArrayList<>();


                        //run kmeans as long as max iter not reached or convergence
                        while (counter < maxIter) {

                            List<Tuple2<Point, ClusterCenter>> pointsAndClusters = new ArrayList<>();

                            //classifiy points with minimal distance
                            for (Point p : iterable) {
                                double dist = Integer.MAX_VALUE;

                                for (ClusterCenter c : clusterCenters) {

                                    if (dist > p.euclideanDistance(c)) {

                                        helper = new Tuple2<>(p, c);
                                        dist = p.euclideanDistance(c);

                                    }

                                }
                                pointsAndClusters.add(helper);

                            }

                            int equalClusterCounter = 0;

                            //calculate new cluster centers
                            for (ClusterCenter c : clusterCenters) {

                                double[] calcNewCentroid = new double[c.getFields().length];
                                int total = 0;

                                for (Tuple2<Point, ClusterCenter> tuple : pointsAndClusters) {

                                    if (tuple.f1 == c) {

                                        for (int i = 0; i < c.getFields().length; i++) {

                                            calcNewCentroid[i] += tuple.f1.getFields()[i];
                                        }
                                        total++;
                                    }
                                }

                                int fieldCounter = 0;

                                for (int j = 0; j < c.getFields().length; j++) {
                                    calcNewCentroid[j] = calcNewCentroid[j] / total;
                                    if (calcNewCentroid[j] == c.getFields()[j]) {
                                        fieldCounter++;
                                    }
                                }
                                c.setFields(calcNewCentroid);
                                if (fieldCounter == c.getFields().length) {

                                    equalClusterCounter++;

                                }

                            }
                            finalList = pointsAndClusters;
                            //end calculation if convergence reached
                            if (equalClusterCounter == k) {

                                break;
                            }
                            counter++;
                        }

                        //collect list of points with associated cluster centers
                        for (Tuple2<Point, ClusterCenter> pc : finalList) {
                            collector.collect(pc);
                        }
                    }
                });

        //result.writeAsText("C:/Users/Laura/documents/DFKI/code/kMeans/data/results.txt");
        //env.execute();

        //System.out.println(env.getExecutionPlan());


        DataStream<Tuple1<Integer>> count = clusters.map(new MapFunction<Tuple2<Point, ClusterCenter>, Tuple1<Integer>>() {
            @Override
            public Tuple1<Integer> map(Tuple2<Point, ClusterCenter> pointClusterCenterTuple2) throws Exception {
                return new Tuple1<Integer>(1);
            }
        }).windowAll(SlidingProcessingTimeWindows.of(Time.milliseconds(40), Time.milliseconds(10))).sum(0);



        count.writeAsCsv("/home/eleicha/Repos/BDAPRO_neu/BDAPRO_optimized_visualization_pipeline/data/out/VDDA/yellow_tripdata_2017-12/5/");

        try {
            env.execute("Streaming Iteration Example");
        } catch (Exception e) {
            throw new BusinessException(e.getMessage(), e);
        }

        timer.stop();
        log.info("Time for KMeans VDDA " + timer.getTime() + "ms");

        return clusters;


    }

    @Override
    public DataStream<Point> scatterPlot(int x, int y) {

        StopWatch timer = new StopWatch();
        timer.start();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<String> dataStream;
        // execute the program

        //read input file
        dataStream = env.readTextFile("/home/eleicha/Repos/BDAPRO_neu/BDAPRO_optimized_visualization_pipeline/data/yellow_tripdata_2017-12.csv");

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
                .windowAll(SlidingProcessingTimeWindows.of(Time.milliseconds(40), Time.milliseconds(10)))
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

                        int ydiv = yMax - yMin;
                        int xdiv = xMax - xMin;

                        for (Tuple2<Integer, Integer> v : iterable) {
                            if (ydiv != 0){
                                yVal = Math.round(y * (v.f1 - yMin) / ydiv);
                            }else {
                                yVal = Math.round(y * (v.f1 - yMin) / 1);
                            }
                            if (xdiv != 0){
                                xVal = Math.round(x * (v.f0 - xMin) / xdiv);
                            }else {
                                xVal = Math.round(x * (v.f0 - xMin) / 1);

                            }

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

        timer.stop();
        log.info("Time for VDDA " + timer.getTime() + "ms");

        return points;

    }

}