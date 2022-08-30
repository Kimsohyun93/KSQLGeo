package com.github.wlaforest.ksql.udf;

import com.github.wlaforest.geo.GeometryParseException;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;

class Carriage{
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

@UdafDescription(
        name = "intersected_circle",
        description = "UDF function to test for geometry intersection in euclidean space. geometry encoded in " +
                "WKT or GeoJSON. null value result in false being returned.",
        version = "1.3.1",
        author = "Will LaForest"
)
public final class GeoIntersectedCircle extends GeometryBase{
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

  @UdafFactory(description = "check polygon intersected",
          paramSchema = PARAM_SCHEMA_DESCRIPTOR,
          returnSchema = RETURN_SCHEMA_DESCRIPTOR)
  public Udaf<Struct, Map<String, String>, Struct> createUdaf() {

    return new Udaf<Struct, Map<String, String>,Struct>() {

      @Override
      public Map<String, String> initialize() {
        final Map<String, String> stats = new HashMap<>();
        return stats;
      }

      @Override
      public Map<String, String> aggregate(
              final Struct newValue,
              final Map<String, String> aggregateValue
      ) {
        final String aeName = newValue.getString(AE);
        final String cntName = newValue.getString(CNT);
        final String polygon = newValue.getString(POLYGON);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put(AE, aeName);
        jsonObject.put(CNT, cntName);

        System.out.println(aeName + cntName + polygon);
        aggregateValue.put(jsonObject.toJSONString(),polygon);
        return aggregateValue;
      }


      @Override
      public Map<String, String> merge(
              final Map<String, String> aggOne,
              final Map<String, String> aggTwo
      ) {
        System.out.println("========== MERGE FUNCTION");
        return aggOne;
      }

      @Override
      public Struct map(final Map<String, String> agg) {
        JSONParser jsonParser = new JSONParser();
        Struct result = new Struct(RETURN_SCHEMA);

        Map<String, String> sortedMap = new TreeMap<>(agg);
        boolean intersect_response;
        Map<JSONObject, JSONArray> intersected_result = new HashMap<>();
        JSONObject key1Resource = null;
        JSONObject key2Resource = null;


        System.out.println("========== AGG KEYSET");
        System.out.println(sortedMap.keySet());

//
//        for(String key1 : sortedMap.keySet()){
//          try {
//            Object obj = jsonParser.parse(key1);
//
//            if (obj instanceof JSONObject) {
//              key1Resource = (JSONObject)obj;
//            }
//          } catch (ParseException e) {
//            e.printStackTrace();
//          }
//
//          for(String key2 : sortedMap.keySet()){
//            try {
//              Object obj = jsonParser.parse(key2);
//
//              if (obj instanceof JSONObject) {
//                key2Resource = (JSONObject)obj;
//              }
//            } catch (ParseException e) {
//              e.printStackTrace();
//            }
//
//            try {
//              if(!Objects.equals(key1, key2)){
//                intersect_response = getSpatial4JHelper().intersect(sortedMap.get(key1), sortedMap.get(key2));
//
//                if(intersect_response){
//
//                  JSONArray itstedArrlist1 = intersected_result.getOrDefault(key1Resource, new JSONArray());
//                  itstedArrlist1.add(key2Resource);
//                  intersected_result.put(key1Resource, itstedArrlist1);
//
//                  JSONArray itstedArrlist2 = intersected_result.getOrDefault(key2Resource, new JSONArray());
//                  itstedArrlist2.add(key1Resource);
//                  intersected_result.put(key2Resource, itstedArrlist2);
//                }
//              }
//            } catch (GeometryParseException e) {
//              e.printStackTrace();
//            }
//          }
//          sortedMap.remove(key1);
////          result.put(RESOURCE, key1Resource.toString());
////          result.put(INTERSECTED, intersected_result.get(key1Resource).toString());
//          result.put(key1Resource.toJSONString(), intersected_result.get(key1Resource).toJSONString());
//        }

        System.out.println(result);
        return result;
      }
    };
  }
}