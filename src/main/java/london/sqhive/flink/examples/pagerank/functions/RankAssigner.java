package london.sqhive.flink.examples.pagerank.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * A map function that assigns an initial rank to all pages.
 */
public final class RankAssigner
    implements MapFunction<Long, Tuple2<Long, Double>> {

    Tuple2<Long, Double> outPageWithRank;

    public RankAssigner(double rank) {
        this.outPageWithRank = new Tuple2<Long, Double>(-1L, rank);
    }

    @Override
    public Tuple2<Long, Double> map(Long page) {
        outPageWithRank.f0 = page;
        return outPageWithRank;
    }
}