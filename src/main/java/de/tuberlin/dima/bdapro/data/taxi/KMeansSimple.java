package de.tuberlin.dima.bdapro.data.taxi;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import de.tuberlin.dima.bdapro.data.StreamProcessor;
import de.tuberlin.dima.bdapro.error.BusinessException;
import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.Point;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Calculate k-means without data reduction
 */

@Slf4j
public class KMeansSimple extends StreamProcessor {

    /**
     * gets execution environment
     * @param env get streaming environment
     */

    public KMeansSimple(StreamExecutionEnvironment env) {
		super(env);
    }

    /**
     *
     * calculates k-means over the full data stream
     *
     * @param xBound the number of pixels in x direction
     * @param yBound the number of pixels in y direction
     * @param k the number of clusters
     * @param maxIter the number of maximal iterations done by k-means
     * @return a data stream with a timestamp for each point, the point with its coordinates, and the assigned cluster center with its coordinates
     */

    @Override
    public DataStream<Tuple3<LocalDateTime,Point, ClusterCenter>> cluster(int xBound, int yBound, int k, int maxIter, Time window, Time slide) {

        StopWatch timer = new StopWatch();
        timer.start();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> dataPoints = scatterPlot(xBound, yBound, window, slide);

        //initialize the first points as cluster centers
        DataStream<Tuple3<LocalDateTime, Point, ClusterCenter>> clusters = dataPoints
                .windowAll(TumblingEventTimeWindows.of(window))
                .apply(new AllWindowFunction<Tuple4<LocalDateTime, Double, Point, Integer>, Tuple3<LocalDateTime, Point, ClusterCenter>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple4<LocalDateTime, Double, Point, Integer>> iterable, Collector<Tuple3<LocalDateTime, Point, ClusterCenter>> collector) throws Exception {

                        int counter = 0;
                        List<ClusterCenter> clusterCenters = new ArrayList<>();

                        //take the first k points as initial cluster centers
                        for (Tuple4<LocalDateTime, Double, Point, Integer> p : iterable) {
                            if (counter < k) {

                                clusterCenters.add(new ClusterCenter(counter, p.f2));
                                counter++;

                            }
                        }

                        Tuple3<LocalDateTime,Point, ClusterCenter> helper = new Tuple3<>();
                        List<Tuple3<LocalDateTime,Point, ClusterCenter>> finalList = new ArrayList<>();


                        //run kmeans as long as max iter not reached or convergence
                        for (int iter = 0; iter < maxIter; iter++) {

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

                                    if (t.f2 == c) {

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
                        }

                        //collect list of points with associated cluster centers
                        for (Tuple3<LocalDateTime,Point, ClusterCenter> pc : finalList) {
                            collector.collect(pc);
                        }
                    }
                });

        timer.stop();

        //cluster ids, clusters with x and y coordinates, a count for how many points are associated with a cluster and the starting time of the window
        DataStream<Tuple5<Integer,Double,Double, Integer, Long>> clusterCenters = clusters
                .windowAll(TumblingEventTimeWindows.of(window)).apply(new AllWindowFunction<Tuple3<LocalDateTime, Point, ClusterCenter>, Tuple5<Integer,Double,Double, Integer, Long>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple3<LocalDateTime, Point, ClusterCenter>> iterable, Collector<Tuple5<Integer,Double,Double, Integer, Long>> collector) throws Exception {

                        HashMap<Integer, Tuple2<ClusterCenter, Integer>> pointsInClusters = new HashMap<>();

                        for (Tuple3<LocalDateTime, Point, ClusterCenter> c: iterable){
                            //check if value already there, if yes, only add count
                            pointsInClusters.computeIfPresent(c.f2.getId(), (k, value) -> new Tuple2<>(value.f0, value.f1+1));
                            //only save the first incoming point per window
                            pointsInClusters.putIfAbsent(c.f2.getId(), new Tuple2<>(c.f2, 1));
                        }

                        pointsInClusters.entrySet().stream()
                                .forEach(e -> {
                                    collector.collect(new Tuple5<>(e.getValue().f0.getId(), e.getValue().f0.getFields()[0], e.getValue().f0.getFields()[1], e.getValue().f1, timeWindow.getStart()));
                                });
                    }
                });

        DataStream<Tuple1<Integer>> count = clusters.map(new MapFunction<Tuple3<LocalDateTime, Point, ClusterCenter>, Tuple1<Integer>>() {
            @Override
            public Tuple1<Integer> map(Tuple3<LocalDateTime, Point, ClusterCenter> tuple) throws Exception {
                return new Tuple1<>(1);
            }
        }).windowAll(TumblingEventTimeWindows.of(window)).sum(0);

        log.info("Time for KMeans plain " + timer.getTime() + "ms");

        clusterCenters.writeAsCsv("data/out/Simple/yellow_tripdata_2017-12/finalEval/cluster/");
        count.writeAsCsv("data/out/Simple/yellow_tripdata_2017-12/finalEval/count/");

        try {
            env.execute("Streaming Iteration Example");
        } catch (Exception e) {
            throw new BusinessException(e.getMessage(), e);
        }

        return clusters;


    }

    /**
     *
     * Transforms the inputted data into a data stream for the scatter plot
     *
     * @param x Number of Pixels in the x direction
     * @param y Number of Pixels in the y direction
     * @return data stream with a timestamp, a key for the display bucket which is set to 0.0 for this case, the coordinates of the point, and the number of points inside one bucket which will always be 1 in this case
     */

    @Override
    public DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> scatterPlot(int x, int y, Time window, Time slide) {

        StopWatch timer = new StopWatch();
        timer.start();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> dataStream;
        // execute the program

        //read input file
        dataStream = env.readTextFile("data/yellow_tripdata_2017-12_small.csv");

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
                        String x1 = helper[10].replaceAll(",", ".").replaceAll("^\"|\"$", "").replaceAll("^\"|\"$", "");
                        String y1 = helper[13].replaceAll(",", ".").replaceAll("^\"|\"$", "").replaceAll("^\"|\"$", "");
                        Tuple3<LocalDateTime, Double, Double> output = new Tuple3<>(dateTime, Double.valueOf(x1), Double.valueOf(y1));//date, fare amount, and tip amount
                        out.collect(output);
                    }
                }
            }
        });

        //apply simple stream transformation with timestamp assignment and tumbling event time window
        DataStream<Tuple4<LocalDateTime, Double, Point, Integer>> points = pDataStream
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<LocalDateTime,Double,Double>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<LocalDateTime,Double, Double> event) {
                        long seconds = event.f0.getSecond()*1000L;
                        long min = event.f0.getMinute()*60L*1000L;
                        long hr = event.f0.getHour()*60L*60L*1000L;
                        long day = event.f0.getDayOfYear()*60L*60L*24L*1000L;
                        long year = event.f0.getYear()*60L*60L*24L*365L*1000L;

                        return seconds+min+hr+day+year;
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(window))
                .apply(new AllWindowFunction<Tuple3<LocalDateTime, Double, Double>, Tuple4<LocalDateTime, Double, Point, Integer>, TimeWindow>()  {


                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple3<LocalDateTime, Double, Double>> iterable, Collector<Tuple4<LocalDateTime, Double, Point, Integer>> collector) throws Exception {

                        for (Tuple3<LocalDateTime, Double, Double> v: iterable){

                            double[] data = new double[]{v.f1, v.f2};

                            Point point = new Point(data);

                            collector.collect(new Tuple4<LocalDateTime, Double, Point, Integer>(v.f0,0.0,point,1));
                        }

                    }
                });


        try {
            env.execute("Streaming Iteration Example");
        } catch (Exception e) {
            throw new BusinessException(e.getMessage(), e);
        }

        timer.stop();
        log.info("Time for Streamprocessing " + timer.getTime() + "ms");

        return points;
    }


}
