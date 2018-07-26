/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package london.sqhive.flink.examples.pagerank;

import london.sqhive.flink.examples.pagerank.functions.*;
import london.sqhive.flink.examples.pagerank.data.PageRankData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

public class Application {

	private static final double DAMPENING_FACTOR = 0.85;
	private static final double EPSILON = 0.0001;

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);

		final int numPages = params.getInt("numPages", PageRankData.getNumberOfPages());
		final int maxIterations = params.getInt("iterations", 10);

		// set up execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make the parameters available to the web ui
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataSet<Long> pagesInput = getPagesDataSet(env, params);
		DataSet<Tuple2<Long, Long>> linksInput = getLinksDataSet(env, params);

		// assign initial rank to pages
		DataSet<Tuple2<Long, Double>> pagesWithRanks = pagesInput.
				map(new RankAssigner((1.0d / numPages)));

		// build adjacency list from link input
		DataSet<Tuple2<Long, Long[]>> adjacencyListInput =
				linksInput.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());

		// set iterative data set
		IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(maxIterations);

		DataSet<Tuple2<Long, Double>> newRanks = iteration
				// join pages with outgoing edges and distribute rank
				.join(adjacencyListInput).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch())
				// collect and sum ranks
				.groupBy(0).aggregate(SUM, 1)
				// apply dampening factor
				.map(new Dampener(DAMPENING_FACTOR, numPages));

		DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith(
			newRanks,
			newRanks.join(iteration)
					.where(0)
					.equalTo(0)
					// termination condition
					.filter(new EpsilonFilter(EPSILON))
		);

		// emit result
		if (params.has("output")) {
			finalPageRanks.writeAsCsv(params.get("output"), "\n", " ");
			// execute program
			env.execute("Basic Page Rank Example");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			finalPageRanks.print();
		}
	}

	private static DataSet<Long> getPagesDataSet(ExecutionEnvironment env, ParameterTool params) {
		if (params.has("pages")) {
			return env.readCsvFile(params.get("pages"))
					.fieldDelimiter(" ")
					.lineDelimiter("\n")
					.types(Long.class)
					.map(new MapFunction<Tuple1<Long>, Long>() {
						@Override
						public Long map(Tuple1<Long> v) {
							return v.f0;
						}
					});
		} else {
			System.out.println("Executing PageRank example with default pages data set.");
			System.out.println("Use --pages to specify file input.");
			return PageRankData.getDefaultPagesDataSet(env);
		}
	}

	private static DataSet<Tuple2<Long, Long>> getLinksDataSet(ExecutionEnvironment env, ParameterTool params) {
		if (params.has("links")) {
			return env.readCsvFile(params.get("links"))
					.fieldDelimiter(" ")
					.lineDelimiter("\n")
					.types(Long.class, Long.class);
		} else {
			System.out.println("Executing PageRank example with default links data set.");
			System.out.println("Use --links to specify file input.");
			return PageRankData.getDefaultEdgeDataSet(env);
		}
	}
}
