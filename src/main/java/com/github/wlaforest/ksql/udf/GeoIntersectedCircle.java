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
import org.json.simple.parser.ParseException;

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
  public static Udaf<Struct,Map<String, List<String>>, Map<String, List<String>>> createUdaf() {

    return new Udaf<Struct,Map<String, List<String>>, Map<String, List<String>>>() {

      @Override
      public Map<String, List<String>> initialize() {
//        Map<Object, Object> list = new LinkedHashMap<>();
//        System.out.println("THIS IS INITIALIZE");
        return new LinkedHashMap<>();
      }

      @Override
      public Map<String, List<String>> aggregate(
              final Struct newValue,
              final Map<String, List<String>> aggregateValue
      ) {
        final String aeName = newValue.getString(AE);
        final String cntName = newValue.getString(CNT);
        final String polygon = newValue.getString(POLYGON);
        System.out.println("THIS IS AGGREGATE : " + aeName +" " + cntName);
        JSONObject jsonObject = new JSONObject();
        JSONObject valueObject = new JSONObject();
        jsonObject.put(AE, aeName);
        jsonObject.put(CNT, cntName);
        jsonObject.put(POLYGON, polygon);

        valueObject.put(AE, aeName);
        valueObject.put(CNT, cntName);

        JSONObject key = new JSONObject();
        JSONObject value = new JSONObject();
        boolean intersect_response = false;
        List<String> li = new ArrayList<>();
        String[] arr = {};
        // aggregateValue - KEY : {AE, CNT, POLYGON} / VALUE : [{AE,CNT}(INTERSECTED ARRAY)]

        aggregateValue.put(jsonObject.toJSONString(),li);

        for(String tmpKey : aggregateValue.keySet()){

          // Map<String, String>으로 저장되어 있는 key를 JSONObject로 변환시킴
          try {
            key = (JSONObject) jsonParser.parse(tmpKey);
          } catch (ParseException e) {
            e.printStackTrace();
          }

          // newValue와 aggregateValue 의 AE,CNT 값이 같으면 continue
          if(key.get(AE).equals(aeName) && key.get(CNT).equals(cntName)){
            continue;
          }

          // 두 폴리건 사이의 Intersect를 구함 (Boolean)
          try {
            intersect_response = s4h.intersect(key.get(POLYGON).toString(), polygon);
          } catch (GeometryParseException e) {
            e.printStackTrace();
          }

          System.out.println("GET INTERSECTED RESPONSE : " );
          System.out.println("NEW VALUE : {" + aeName + ", " + cntName + "} , COMPARE VALUE : {" + key.get(AE) + ", " + key.get(CNT) + "} , RESULT : " + intersect_response);



          // Intersect 결과에 따라 두 Value에 {AE,CNT} 쌍을 입력할지, 삭제할지 결정
          if(intersect_response){
            System.out.println("THIS IS INTERSECTED");
            System.out.println("THIS IS VALUE OBJECT : " + valueObject);

            System.out.println("중복체크 : " + !aggregateValue.get(tmpKey).contains(valueObject.toJSONString()));
            if(!aggregateValue.get(tmpKey).contains(valueObject.toJSONString())) {
              li = aggregateValue.get(tmpKey);
              System.out.println("THIS IS LI## : " + li);
              li.add(valueObject.toJSONString());
              System.out.println("THIS IS LI#### : " + li);
              aggregateValue.put(tmpKey, li);
            }

            li = aggregateValue.get(jsonObject.toJSONString());
            li.add(key.remove(POLYGON).toString());
            aggregateValue.put(jsonObject.toJSONString(), li);

          }else{
            System.out.println("THIS IS NOT INTERSECTED");
            System.out.println("THIS IS CONTAINES RESULT : " + aggregateValue.get(tmpKey).contains(valueObject.toJSONString()));
            if(aggregateValue.get(tmpKey).contains(valueObject.toJSONString())){
              li = aggregateValue.get(tmpKey);
              li.remove(valueObject.toJSONString());
              aggregateValue.put(tmpKey, li);

              li = aggregateValue.get(jsonObject.toJSONString());
              li.remove(key.remove(POLYGON).toString());
              aggregateValue.put(jsonObject.toJSONString(), li);
            }
          }
        }
        return aggregateValue;
      }


      @Override
      public Map<String, List<String>>  merge(
              final Map<String, List<String>> aggOne,
              final Map<String, List<String>> aggTwo
      ) {
        System.out.println("========== MERGE FUNCTION");
        return aggOne;
      }

      @Override
      public Map<String, List<String>> map(final Map<String, List<String>> agg) {
        // 내 group (AE, CNT)에 맞는 애들만 반환하고 싶은데

//        System.out.println("THIS IS MAP : " + agg);
        Map<String, List<String>> returnAgg = new LinkedHashMap<>();
        JSONObject returnKey = new JSONObject();
        for(String key : agg.keySet()){
          try {
            returnKey = (JSONObject) jsonParser.parse(key);
          } catch (ParseException e) {
            e.printStackTrace();
          }
          returnKey.remove(POLYGON);
          returnAgg.put(returnKey.toJSONString(), agg.get(key));
        }

        return returnAgg;
      }
    };
  }
}