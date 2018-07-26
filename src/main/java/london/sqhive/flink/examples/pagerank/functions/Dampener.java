package london.sqhive.flink.examples.pagerank.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * The function that applies the page rank dampening formula.
 */
@FunctionAnnotation.ForwardedFields("0")
public final class Dampener
    implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

    private final double dampening;
    private final double randomJump;

    public Dampener(double dampening, double numVertices) {
        this.dampening = dampening;
        this.randomJump = (1 - dampening) / numVertices;
    }

    @Override
    public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
        value.f1 = (value.f1 * dampening) + randomJump;
        return value;
    }
}