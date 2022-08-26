package com.github.wlaforest.ksql.udf;


import com.github.wlaforest.geo.GeometryParseException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(
        name = "intersected_circle",
        description = "UDF function to test for geometry intersection in euclidean space. geometry encoded in " +
                "WKT or GeoJSON. null value result in false being returned.",
        version = "1.3.1",
        author = "Will LaForest"
)
public class GeoIntersectedCircle extends GeometryBase {
  @Udf(description = "determines if a the two geometries intersect.")
  public boolean intersected_circle (
          @UdfParameter(value = "polygon", description = "WKT or GeoJSON Encoded Geometry to check for intersection with polygon") final String polygon)
          throws GeometryParseException {

    System.out.println(polygon);
    return true;
  }
}
