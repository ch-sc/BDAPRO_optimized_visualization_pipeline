package de.tuberlin.dima.bdapro.data.taxi;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.error.BusinessException;
import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.Point;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
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

@Slf4j
public class KMeansVDDA extends StreamProcessor{

    private final StreamExecutionEnvironment env;


    public KMeansVDDA(StreamExecutionEnvironment env) {
        this.env = env;
    }

    @Override
    public DataStream<Tuple3<LocalDateTime,Point, ClusterCenter>> cluster(int xBound, int yBound, int k, int maxIter) {

        StopWatch timer = new StopWatch();
        timer.start();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> dataPoints = scatterPlot(xBound, yBound);

        //initialize the first points as cluster centers
        DataStream<Tuple3<LocalDateTime, Point, ClusterCenter>> clusters = dataPoints
                .keyBy(0).window(SlidingEventTimeWindows.of(Time.hours(5),Time.hours(2)))
                .apply(new WindowFunction<Tuple4<LocalDateTime, Double, Point, Integer>, Tuple3<LocalDateTime, Point, ClusterCenter>, Tuple, TimeWindow>() {


                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<LocalDateTime, Double, Point, Integer>> iterable, Collector<Tuple3<LocalDateTime, Point, ClusterCenter>> collector) throws Exception {

                        int counter = 0;
                        List<ClusterCenter> clusterCenters = new ArrayList<>();

                        //take the first k points as initial cluster centers
                        for (Tuple4<LocalDateTime, Double, Point, Integer> p : iterable) {
                            if (counter < k) {

                                clusterCenters.add(new ClusterCenter(counter, p.f2));
                                counter++;

                            }
                        }

                        counter = 0;
                        Tuple3<LocalDateTime,Point, ClusterCenter> helper = new Tuple3<>();
                        List<Tuple3<LocalDateTime,Point, ClusterCenter>> finalList = new ArrayList<>();


                        //run kmeans as long as max iter not reached or convergence
                        while (counter < maxIter) {

                            List<Tuple3<LocalDateTime,Point, ClusterCenter>> pointsAndClusters = new ArrayList<>();

                            //classifiy points with minimal distance
                            for (Tuple4<LocalDateTime, Double, Point, Integer> p : iterable) {
                                double dist = Integer.MAX_VALUE;

                                for (ClusterCenter c : clusterCenters) {

                                    if (dist > p.f2.euclideanDistance(c)) {

                                        helper = new Tuple3<LocalDateTime,Point, ClusterCenter>(p.f0, p.f2, c);
                                        dist = p.f2.euclideanDistance(c);

                                    }

                                }
                                pointsAndClusters.add(helper);

                            }

                            int equalClusterCounter = 0;

                            //calculate new cluster centers
                            for (ClusterCenter c : clusterCenters) {

                                double[] calcNewCentroid = new double[c.getFields().length];
                                int total = 0;

                                for (Tuple3<LocalDateTime,Point, ClusterCenter> t : pointsAndClusters) {

                                    if (t.f1 == c) {

                                        for (int i = 0; i < c.getFields().length; i++) {

                                            calcNewCentroid[i] += t.f1.getFields()[i];
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
                        for (Tuple3<LocalDateTime,Point, ClusterCenter> pc : finalList) {
                            collector.collect(pc);
                        }
                    }
                });

        //result.writeAsText("C:/Users/Laura/documents/DFKI/code/kMeans/data/results.txt");
        //env.execute();

        //System.out.println(env.getExecutionPlan());


        DataStream<Tuple1<Integer>> count = clusters.map(new MapFunction<Tuple3<LocalDateTime, Point, ClusterCenter>, Tuple1<Integer>>() {
            @Override
            public Tuple1<Integer> map(Tuple3<LocalDateTime, Point, ClusterCenter> tuple) throws Exception {
                return new Tuple1<>(1);
            }
        }).windowAll(SlidingEventTimeWindows.of(Time.hours(5),Time.hours(2))).sum(0);


        count.writeAsCsv("/home/eleicha/Repos/BDAPRO_neu/BDAPRO_optimized_visualization_pipeline/data/out/VDDA/100000/2/");

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
    public DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> scatterPlot(int x, int y) {

        StopWatch timer = new StopWatch();
        timer.start();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> dataStream;
        // execute the program

        //read input file
        dataStream = env.readTextFile("/home/eleicha/Repos/BDAPRO_neu/BDAPRO_optimized_visualization_pipeline/data/100000.csv");

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
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<LocalDateTime,Double,Double>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<LocalDateTime,Double, Double> event) {
                return Long.valueOf(event.f0.getSecond()+event.f0.getMinute()+event.f0.getHour()+event.f0.getDayOfYear()+event.f0.getYear());
            }
        }).keyBy(0).window(SlidingEventTimeWindows.of(Time.hours(5),Time.hours(2)))
                .apply(new WindowFunction<Tuple3<LocalDateTime, Double, Double>, Tuple4<LocalDateTime, Double, Point, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<LocalDateTime, Double, Double>> iterable, Collector<Tuple4<LocalDateTime, Double, Point, Integer>> collector) throws Exception {


                        double xMax = Double.MIN_VALUE;
                        double xMin = Double.MAX_VALUE;
                        double yMax = Double.MIN_VALUE;
                        double yMin = Double.MAX_VALUE;
                        double yVal = 0;
                        double xVal = 0;
                        Map<Double, Tuple3<LocalDateTime,Point, Integer>> valueMap = new HashMap<>();

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
                            if (ydiv != 0){
                                yVal = Math.round(y * (v.f2 - yMin) / ydiv);
                            }else {
                                yVal = Math.round(y * (v.f2 - yMin) / 1);
                            }
                            if (xdiv != 0){
                                xVal = Math.round(x * (v.f1 - xMin) / xdiv);
                            }else {
                                xVal = Math.round(x * (v.f1 - xMin) / 1);

                            }

                            double key = x* yVal + xVal;
                            double[] data = new double[]{v.f1, v.f2};

                            Point point = new Point(data);

                            //count number of occurences
                            valueMap.computeIfPresent(key, (k, value) -> new Tuple3<LocalDateTime,Point,Integer>(value.f0, value.f1, value.f2+1));
                            //only save the first incoming point per window
                            valueMap.putIfAbsent(key, new Tuple3<LocalDateTime,Point,Integer>(v.f0, point, 1));
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