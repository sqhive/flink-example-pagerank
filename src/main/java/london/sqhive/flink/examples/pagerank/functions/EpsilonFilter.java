package london.sqhive.flink.examples.pagerank.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Filter that filters vertices where the rank difference is below a threshold.
 */
public final class EpsilonFilter
    implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

    private double EPSILON;

    public EpsilonFilter(double epsilon) {
        super();

        EPSILON = epsilon;
    }

    @Override
    public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) {
        return Math.abs(value.f0.f1 - value.f1.f1) > EPSILON;
    }
}