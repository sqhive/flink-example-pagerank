package london.sqhive.flink.examples.pagerank.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * A reduce function that takes a sequence of edges and builds the adjacency list for the vertex where the edges
 * originate. Run as a pre-processing step.
 */
@FunctionAnnotation.ForwardedFields("0")
public final class BuildOutgoingEdgeList
    implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

    private final ArrayList<Long> neighbors = new ArrayList<Long>();

    @Override
    public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) {
        neighbors.clear();
        Long id = 0L;

        for (Tuple2<Long, Long> n : values) {
            id = n.f0;
            neighbors.add(n.f1);
        }
        out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
    }
}