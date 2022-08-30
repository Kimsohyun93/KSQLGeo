package com.github.wlaforest.ksql.udf;

import com.github.wlaforest.geo.GeometryParseException;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.lang.reflect.Array;
import java.util.*;


@UdafDescription(
        name = "intersected_circle",
        description = "UDF function to test for geometry intersection in euclidean space. geometry encoded in " +
                "WKT or GeoJSON. null value result in false being returned.",
        version = "0.1.0-SNAPSHOT",
        author = "shkim"
)
public final class  GeoIntersectedCircle extends GeometryBase {

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
  private static final String RESOURCE = "RESOURCE";
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
          .field(RESOURCE, Schema.OPTIONAL_STRING_SCHEMA)
          .field(INTERSECTED,Schema.OPTIONAL_STRING_SCHEMA)
          .build();

  public static final String RETURN_SCHEMA_DESCRIPTOR = "STRUCT<" +
          "RESOURCE STRING," +
          "INTERSECTED STRING" +
          ">";
  private GeoIntersectedCircle() {
  }

  @UdafFactory(description = "compute the slope and find the inflection points",
          paramSchema = PARAM_SCHEMA_DESCRIPTOR,
          returnSchema = RETURN_SCHEMA_DESCRIPTOR)
  public Udaf<Struct, Map<Carriage, String>, Struct> createUdaf() {

    return new Udaf<Struct, Map<Carriage, String>, Struct>() {
      @Override
      public Map<Carriage, String> initialize() {

        final Map<Carriage, String> data = new HashMap<>();
        return data;
      }


      @Override
      public Map<Carriage, String> aggregate(
              final Struct newValue,
              final Map<Carriage, String> aggregateValue
      ) {
        System.out.println("AGGREGATE FUNCTION NEW VALUE");
        System.out.println(newValue);

        final String aeName = newValue.getString(AE);
        final String cntName = newValue.getString(CNT);
        final String polygon = newValue.getString(POLYGON);

        aggregateValue.put(new Carriage(aeName, cntName),polygon);

        return aggregateValue;
      }


      @Override
      public Map<Carriage, String> merge(
              final Map<Carriage, String> aggOne,
              final Map<Carriage, String> aggTwo
      ) {
        System.out.println("========== MERGE FUNCTION");

        return aggOne;
      }

      @Override
      public Struct map(final Map<Carriage, String> agg) {

        Struct result = new Struct(RETURN_SCHEMA);
        // 키로 정렬
        Map<Carriage, String> sortedMap = new TreeMap<>(agg);
        boolean intersect_response;
        Map<Map<String, String>, ArrayList<Map<String, String>>> intersected_result = new HashMap<>();

        System.out.println("========== AGG KEYSET");
        System.out.println(sortedMap.keySet());


        for(Carriage key1 : sortedMap.keySet()){
          Map<String, String> key1Resource = new HashMap<>();
          key1Resource.put(AE, key1.ae);
          key1Resource.put(CNT, key1.cnt);

          for(Carriage key2 : sortedMap.keySet()){
            Map<String, String> key2Resource = new HashMap<>();
            key2Resource.put(AE, key2.ae);
            key2Resource.put(CNT, key2.cnt);

            try {
              if(key1 != key2){
                intersect_response = getSpatial4JHelper().intersect(sortedMap.get(key1), sortedMap.get(key2));

                if(intersect_response){

                  ArrayList<Map<String, String>> itstedArrlist1 = intersected_result.getOrDefault(key1Resource, new ArrayList<>());
                  itstedArrlist1.add(key2Resource);
                  intersected_result.put(key1Resource, itstedArrlist1);

                  ArrayList<Map<String, String>> itstedArrlist2 = intersected_result.getOrDefault(key2Resource, new ArrayList<>());
                  itstedArrlist2.add(key1Resource);
                  intersected_result.put(key2Resource, itstedArrlist2);
                }
              }
            } catch (GeometryParseException e) {
              e.printStackTrace();
            }
          }
          sortedMap.remove(key1);
//          result.put(RESOURCE, key1Resource.toString());
//          result.put(INTERSECTED, intersected_result.get(key1Resource).toString());
          result.put(key1Resource.toString(), intersected_result.get(key1Resource).toString());
        }

        System.out.println(result);
        return result;
      }
    };
  }
}
//
//@UdfDescription(
//        name = "intersected_circle",
//        description = "UDF function to test for geometry intersection in euclidean space. geometry encoded in " +
//                "WKT or GeoJSON. null value result in false being returned.",
//        version = "1.3.1",
//        author = "Will LaForest"
//)
//public class GeoIntersectedCircle extends GeometryBase {
//  public class Carriage{
//    private String ae;
//    private String cnt;
//
//    public Carriage(String ae, String cnt){
//      this.ae = ae;
//      this.cnt =cnt;
//    }
//
//    public String getAe(){
//      return this.ae;
//    }
//    public String getCnt(){
//      return this.cnt;
//    }
//  }
//
//  @Udf(description = "determines if a the two geometries intersect.")
//  public boolean intersected_circle (
//          @UdfParameter(value = "ae", description = "WKT or GeoJSON Encoded Geometry to check for intersection with polygon") final String ae,
//          @UdfParameter(value = "cnt", description = "WKT or GeoJSON Encoded Geometry to check for intersection with polygon") final String cnt,
//          @UdfParameter(value = "polygon", description = "WKT or GeoJSON Encoded Geometry to check for intersection with polygon") final String polygon)
//          throws GeometryParseException {
//
//    System.out.println("===================================");
//    System.out.println(ae);
//    System.out.println(cnt);
//    System.out.println(polygon);
//    System.out.println("===================================");
//
//
//    Map<Carriage, String> sortedMap = new TreeMap<Carriage, String>();
//    sortedMap.put(new Carriage(ae,cnt), polygon);
//
//
//    return true;
//  }
//
//  public Map<String, Boolean> is_intersected(Map<Carriage, String> polygons){
//
//  }
//}
