package com.github.wlaforest.ksql.udf;

import com.github.wlaforest.geo.GeometryParseException;
import com.github.wlaforest.geo.Spatial4JHelper;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.*;



@UdafDescription(
        name = "intersected_circle",
        description = "UDF function to test for geometry intersection in euclidean space. geometry encoded in " +
                "WKT or GeoJSON. null value result in false being returned.",
        version = "1.3.1",
        author = "Will LaForest"
)
public class GeoIntersectedCircle extends GeometryBase {
    private static final String AE = "AE";
    private static final String CNT = "CNT";
    private static final String RESOURCE_NAME = "RESOURCE_NAME";
    private static final String POLYGON = "POLYGON";
    private static final String INTERSECTED = "INTERSECTED";

    static JSONParser jsonParser = new JSONParser();
    static Spatial4JHelper s4h = new Spatial4JHelper();

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
          paramSchema = PARAM_SCHEMA_DESCRIPTOR)
  public static Udaf<Struct,Map<JSONObject, JSONArray>, String> createUdaf() {

    return new Udaf<Struct, Map<JSONObject, JSONArray>, String>() {

      @Override
      public Map<JSONObject, JSONArray>  initialize() {
//        final Map<String, String> stats = new HashMap<>();
//        return stats;

        Map<JSONObject, JSONArray> list = new HashMap<>();
        return list;
      }

      @Override
      public Map<JSONObject, JSONArray>  aggregate(
              final Struct newValue,
              final Map<JSONObject, JSONArray>  aggregateValue
      ) {
        final String aeName = newValue.getString(AE);
        final String cntName = newValue.getString(CNT);
        final String polygon = newValue.getString(POLYGON);

        JSONObject jsonObject = new JSONObject();
        JSONObject valueObject = new JSONObject();
        jsonObject.put(AE, aeName);
        jsonObject.put(CNT, cntName);
        jsonObject.put(POLYGON, polygon);

        valueObject.put(AE, aeName);
        valueObject.put(CNT, cntName);

        JSONArray tmpValue = new JSONArray();
        boolean intersect_response = false;
        // aggregateValue - KEY : {AE, CNT, POLYGON} / VALUE : [{AE,CNT}(INTERSECTED JSON ARRAY)]

//        System.out.println(aeName + cntName + polygon);
        aggregateValue.put(jsonObject,new JSONArray());

        for(JSONObject key : aggregateValue.keySet()){
          if(key.get(AE).equals(aeName) && key.get(CNT).equals(cntName)){
            continue;
          }

          try {
            intersect_response = s4h.intersect(key.get(POLYGON).toString(), polygon);
          } catch (GeometryParseException e) {
            e.printStackTrace();
          }

          System.out.println("GET INTERSECTED RESPONSE : " );
          System.out.println("NEW VALUE : {" + aeName + ", " + cntName + "} , COMPARE VALUE : {" + key.get(AE) + ", " + key.get(CNT) + "} , RESULT : " + intersect_response);
          if(intersect_response){
            System.out.println("THIS IS INTERSECTED");
            tmpValue = aggregateValue.get(key);
            tmpValue.add(valueObject);
            aggregateValue.put(key, tmpValue);
            tmpValue = aggregateValue.get(jsonObject);
            tmpValue.add(key.remove(POLYGON));
            aggregateValue.put(jsonObject, tmpValue);
          }else{
            System.out.println("THIS IS NOT INTERSECTED");

            if(aggregateValue.get(key).contains(valueObject)){
              tmpValue = aggregateValue.get(key);
              tmpValue.remove(valueObject);
              aggregateValue.put(key, tmpValue);
              tmpValue = aggregateValue.get(jsonObject);
              tmpValue.remove(key.remove(POLYGON));
              aggregateValue.put(jsonObject, tmpValue);
            }
          }
        }
        return aggregateValue;
      }


      @Override
      public Map<JSONObject, JSONArray>  merge(
              final Map<JSONObject, JSONArray> aggOne,
              final Map<JSONObject, JSONArray> aggTwo
      ) {
        System.out.println("========== MERGE FUNCTION");
        return aggOne;
      }

      @Override
      public String map(final Map<JSONObject, JSONArray> agg) {
        // 내 group (AE, CNT)에 맞는 애들만 반환하고 싶은데
        String returnValue = agg.entrySet().toArray()[agg.size() -1].toString();
        return returnValue;
      }
    };
  }
}