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

  public static class Carriage{
        private String ae;
        private String cnt;

        public Carriage(String ae, String cnt){
            this.ae = ae;
            this.cnt =cnt;
        }

        public String getAe(){
            return this.ae;
        }
        public String getCnt(){
            return this.cnt;
        }
    }
    private static final String AE = "AE";
    private static final String CNT = "CNT";
    private static final String RESOURCE_NAME = "RESOURCE_NAME";
    private static final String POLYGON = "POLYGON";
    private static final String INTERSECTED = "INTERSECTED";

    public static final Schema PARAM_SCHEMA = SchemaBuilder.struct().optional()
            .field(AE, Schema.OPTIONAL_STRING_SCHEMA)
            .field(CNT, Schema.OPTIONAL_STRING_SCHEMA)
            .field(POLYGON, Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    public static final String PARAM_SCHEMA_DESCRIPTOR = "STRUCT<" +
            "AE STRING," +
            "CNT STRING," +
            "POLYGON STRING" +
            ">";

    public static final Schema RETURN_SCHEMA = SchemaBuilder.struct().optional()
            .field(RESOURCE_NAME, Schema.OPTIONAL_STRING_SCHEMA)
            .field(INTERSECTED,Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    public static final String RETURN_SCHEMA_DESCRIPTOR = "STRUCT<" +
            "RESOURCE_NAME STRING," +
            "INTERSECTED STRING" +
            ">";

  private GeoIntersectedCircle() {
  }

  @UdafFactory(description = "compute if polygon intersected",
          paramSchema = PARAM_SCHEMA_DESCRIPTOR,
          returnSchema = RETURN_SCHEMA_DESCRIPTOR)
  public static Udaf<Struct, Map<String, Double>, Struct> createUdaf() {

    return new Udaf<Struct, Map<String, Double>,Struct>() {

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
              final Struct newValue,
              final Map<String, Double> aggregateValue
      ) {
        final String aeName = newValue.getString(AE);
        final String cntName = newValue.getString(CNT);
        final String polygon = newValue.getString(POLYGON);

        System.out.println(aeName + cntName + polygon);
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
        System.out.println("========== MERGE FUNCTION");
        return aggOne;
      }

      /**
       * Called to map the intermediate aggregate value to the final output.
       *
       * @param agg the aggregate
       * @return the result of aggregation
       */
      @Override
      public Struct map(final Map<String, Double> agg) {
        Struct result = new Struct(RETURN_SCHEMA);
        return result;
      }
    };
  }
}