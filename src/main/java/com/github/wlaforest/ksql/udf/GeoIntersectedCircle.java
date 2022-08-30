package com.github.wlaforest.ksql.udf;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.*;

@UdafDescription(
        name = "intersected_circle",
        description = "UDF function to test for geometry intersection in euclidean space. geometry encoded in " +
                "WKT or GeoJSON. null value result in false being returned.",
        version = "1.3.1",
        author = "Will LaForest"
)
public final class GeoIntersectedCircle {

  private GeoIntersectedCircle() {
  }

  @UdafFactory(description = "check polygon is intersected")
  /**
   * Can be used with stream aggregations. The input of our aggregation will be doubles,
   * and the output will be a map
   *
   * @param I is the input type of the UDAF. A is the data type of the intermediate storage used to keep track of the state of the UDAF. O is the data type of the return value.
   * @return int[]
   */

  //
  public static Udaf<Double, Map<String, Double>, Map<String, Double>> createUdaf() {

    return new Udaf<Double, Map<String, Double>, Map<String, Double>>() {

      /**
       * Specify an initial value for our aggregation
       *
       * @return the initial state of the aggregate.
       */
      @Override
      public Map<String, Double> initialize() {
        final Map<String, Double> stats = new HashMap<>();
        return stats;
      }

      /**
       * Perform the aggregation whenever a new record appears in our stream.
       *
       * @param newValue the new value to add to the {@code aggregateValue}.
       * @param aggregateValue the current aggregate.
       * @return the new aggregate value.
       */
      @Override
      public Map<String, Double> aggregate(
              final Double newValue,
              final Map<String, Double> aggregateValue
      ) {
        final Double sampleSize = 1.0 + aggregateValue
                .getOrDefault("sample_size", 0.0);

        final Double sum = newValue + aggregateValue
                .getOrDefault("sum", 0.0);

        // calculate the new aggregate
        aggregateValue.put("mean", sum / sampleSize);
        aggregateValue.put("sample_size", sampleSize);
        aggregateValue.put("sum", sum);
        return aggregateValue;
      }

      /**
       * Called to merge two aggregates together.
       * The merge method is only called when merging sessions when session windowing is used.
       *
       * @param aggOne the first aggregate
       * @param aggTwo the second aggregate
       * @return the merged result
       */
      @Override
      public Map<String, Double> merge(
              final Map<String, Double> aggOne,
              final Map<String, Double> aggTwo
      ) {
        final Double sampleSize =
                aggOne.getOrDefault("sample_size", 0.0) + aggTwo.getOrDefault("sample_size", 0.0);
        final Double sum =
                aggOne.getOrDefault("sum", 0.0) + aggTwo.getOrDefault("sum", 0.0);

        // calculate the new aggregate
        final Map<String, Double> newAggregate = new HashMap<>();
        newAggregate.put("mean", sum / sampleSize);
        newAggregate.put("sample_size", sampleSize);
        newAggregate.put("sum", sum);
        return newAggregate;
      }

      /**
       * Called to map the intermediate aggregate value to the final output.
       *
       * @param agg the aggregate
       * @return the result of aggregation
       */
      @Override
      public Map<String, Double> map(final Map<String, Double> agg) {
        return agg;
      }
    };
  }
}