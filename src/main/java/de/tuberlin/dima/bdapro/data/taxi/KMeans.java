package de.tuberlin.dima.bdapro.data.taxi;

import de.tuberlin.dima.bdapro.model.ClusterCenter;
import de.tuberlin.dima.bdapro.model.Point;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class KMeans {

    private final StreamExecutionEnvironment env;


    public KMeans(StreamExecutionEnvironment env) {
        this.env = env;
    }


    @Override
    public DataStream<Point> kMeansCluster(int k, int maxIter, double threshold, boolean convergence, DataStream<Point> dataPoints) {
        

        final long timeStart = System.currentTimeMillis();

        //initialize the first points as cluster centers
        DataStream<ClusterCenter> clusters = dataPoints.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Point>() {
                    @Override
                    public long extractAscendingTimestamp(Point element) {
                        return System.currentTimeMillis();
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(500)))
                .apply(new AllWindowFunction<Point, ClusterCenter, TimeWindow>() {


                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Point> iterable, Collector<ClusterCenter> collector) throws Exception {

                        int counter = 0;

                        for (Point p: iterable) {
                            if (counter < k) {

                                collector.collect((ClusterCenter) p);

                            }

                        }

                    }
                });

        //initialize loop with maxIter to run kmeans either until loop condition is met or until
        //convergence
        IterativeDataStream<ClusterCenter> loop = clusters.iterate(maxIter);

        DataSet<ClusterCenter> newClusters = dataPoints
                //calculate closest centroid for each point
                .map(new NearestCenter()).withBroadcastSet(loop, "clusters")
                // count and sum point coordinates for each centroid
                .map(new AddValueForCounter())
                // group by the centroid ID
                .groupBy(0)
                .reduce(new ClusterAccumulator())
                // compute new centroids from point counts and coordinate sums
                .map(new CalculateNewClusters());

        DataSet<ClusterCenter> finalClusterCenters;

        //check if convergence criterion is inputed
        if (convergence) {
            // Join the new cluster data set with the previous cluster
            DataSet<Tuple2<ClusterCenter, ClusterCenter>> compareSet = newClusters
                    .join(loop)
                    .where("id")
                    .equalTo("id");

            //Evaluate whether the cluster centres are converged (if so, return empty data set)
            DataSet<ClusterCenter> terminationSet = compareSet
                    .flatMap(new ConvergenceEvaluator(threshold));

            // feed new cluster centers back into next iteration
            // If all the clusters are converged, iteration will stop since closeWith stops for empty data sets
            finalClusterCenters = loop.closeWith(newClusters, terminationSet);
        } else {
            finalClusterCenters = loop.closeWith(newClusters);
        }

        // assign points to final clusters
        DataSet<Tuple2<Integer, Point>> result = dataPoints
                .map(new NearestCenter()).withBroadcastSet(finalClusterCenters, "clusters");

        //result.writeAsText("C:/Users/Laura/documents/DFKI/code/kMeans/data/results.txt");
        result.print();
        //env.execute();

        //System.out.println(env.getExecutionPlan());

        long timeEnd = System.currentTimeMillis();
        System.out.println("time diff = " + (timeEnd - timeStart) + " ms");


    }
}

public static class dataPointMaker implements MapFunction<String, Point> {

    /**
     * Convert the String lines into Points
     * @param line String with data seperated by commas
     * @return data Point
     * @throws Exception
     */

    @Override
    public Point map(String line) throws Exception {

        String[] dataPoint = line.split(",");

        double[] data = new double[dataPoint.length];

        for ( int i = 0; i < dataPoint.length; i++){
            data[i] = Double.parseDouble(dataPoint[i]);
        }

        return new Point(data);
    }
}

private static class InitializeClusterCenter implements GroupReduceFunction<Point, ClusterCenter> {

    /**
     * Initialize the cluster centers with randomly chosen data points and label them accordingly with int ids
     * @param values randomly chosen cluster points
     * @param out labeled initial Cluster centers (Points with an ID attached to them)
     * @throws Exception
     */
    @Override
    public void reduce(Iterable<Point> values, Collector<ClusterCenter> out) throws Exception {
        int label = 0;
        for (Point p : values) {
            out.collect(new ClusterCenter(label++, p));
        }
    }
}

private static class NearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {

    private Collection<ClusterCenter> clusters;

    /**
     * Reads the cluster center values from a broadcast variable into a collection.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        this.clusters = getRuntimeContext().getBroadcastVariable("clusters");
    }

    /**
     * Sorts the points to their nearest cluster center according to the euclidean distance
     * @param p one point
     * @return Returns the point together with the id of the nearest cluster center
     * @throws Exception
     */

    @Override
    public Tuple2<Integer, Point> map(Point p) throws Exception {

        double minDistance = Double.MAX_VALUE;
        int closestClusterId = -1;

        // check all cluster centers
        for (ClusterCenter center : clusters) {
            // compute Euclidean distance
            double distance = p.euclideanDistance(center);

            // update nearest cluster if necessary
            if (distance < minDistance) {
                minDistance = distance;
                closestClusterId = center.id;
            }
        }

        // emit a new record with the center id and the data point.
        return new Tuple2<>(closestClusterId, p);
    }
}

private static class AddValueForCounter implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {
    /**
     * Adds a count 1 value to each tuple2, making it a tuple3 with a long value of 1 at the end
     * @param tuple Tuple with the id of a cluster and a point
     * @return a tuple with the cluster id, the Point and a long with value 1
     */
    @Override
    public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> tuple) {
        return new Tuple3<>(tuple.f0, tuple.f1, 1L);
    }
}

public static final class ClusterAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

    /**
     * Sums the points for each cluster center and calculates how many there are per cluster
     * @param val1 first value from the inputed group or previously compined values
     * @param val2 values to compine
     * @return tuple3 with id of cluster, all points in the cluster summed up and a variable with the number of points per cluster
     */

    @Override
    public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
        return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
    }
}

private static class CalculateNewClusters implements MapFunction<Tuple3<Integer,Point,Long>,ClusterCenter>{

    /**
     * Calculates the new centers for each cluster
     * @param summedCluster tuple3 with a cluster id, the summed up points in each cluster, and the number of points per cluster
     * @return the new cluster centers as ClusterCenter
     * @throws Exception
     */
    @Override
    public ClusterCenter map(Tuple3<Integer, Point, Long> summedCluster) throws Exception {

        return new ClusterCenter(summedCluster.f0, summedCluster.f1.divideByScalar(summedCluster.f2));
    }
}

/**
 * Check if the clusters are converged, based on the provided threshold
 */
public static class ConvergenceEvaluator implements FlatMapFunction<Tuple2<ClusterCenter, ClusterCenter>, ClusterCenter> {

    private double threshold;

    public ConvergenceEvaluator(double threshold) {
        this.threshold = threshold;
    }

    /**
     * Evaluates if the threshold criterion is met
     * @param val tuple2 with the previous and current cluster centers for each cluster
     * @param collector collects a data set as return if threshold criterion is not met, yet
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple2<ClusterCenter, ClusterCenter> val, Collector<ClusterCenter> collector) throws Exception {
        if (!evaluateConvergence(val.f0, val.f1, threshold)) {
            collector.collect(val.f0);
        }
    }

    private boolean evaluateConvergence(Point p1, Point p2, double threshold) {
        return (p1.euclideanDistance(p2) <= threshold);
    }
}


}
