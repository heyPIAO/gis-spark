package edu.zju.gis.hls.trajectory.datastore.serializer;

import com.google.gson.*;
import edu.zju.gis.hls.trajectory.datastore.exception.DeserializerException;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.lang.reflect.Type;

/**
 * @author Hu
 * @date 2020/7/12
 **/
public class CRSJsonSerializer implements JsonSerializer<CoordinateReferenceSystem> {

  @Override
  public JsonElement serialize(CoordinateReferenceSystem crs, Type type, JsonSerializationContext jsonSerializationContext) {
    final JsonObject propEle = new JsonObject();
    propEle.addProperty("wkt", crs.toWKT());
    propEle.addProperty("codespace", crs.getName().getCodeSpace());
    propEle.addProperty("code", crs.getName().getCode());
    propEle.addProperty("remarks", crs.getRemarks().toString());
    propEle.addProperty("scope", crs.getScope().toString());
    return propEle;
  }

  public static class CRSJsonDeserializer implements JsonDeserializer<CoordinateReferenceSystem> {

    @Override
    public CoordinateReferenceSystem deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
      final JsonObject root = json.getAsJsonObject();
      CoordinateReferenceSystem crs = null;
      try {
        crs = CRS.parseWKT(root.get("wkt").getAsString());
      } catch (FactoryException e) {
        e.printStackTrace();
        throw new DeserializerException("Deserialize Coordinate Reference System failed with json string: " + root.toString());
      }
      return crs;
    }
  }
}
