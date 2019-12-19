package edu.zju.gis.hls.trajectory.analysis.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.AffineTransformation;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Feature <T extends Geometry> implements Serializable {

  protected String fid;
  protected T geometry;
  protected Map<String, Object> attributes;

  public Object getAttribute(String key){
    return attributes.get(key);
  }

  public Feature(Feature f){
    this(f.getFid(), (T)f.getGeometry(), f.getAttributes());
  }

  /**
   * 偏移
   */
  public void shift(double deltaX, double deltaY) {
    AffineTransformation at = new AffineTransformation();
    at.setToTranslation(deltaX, deltaY);
    this.geometry = (T) at.transform(this.geometry);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("%s \t", fid));
    for(String k: attributes.keySet()){
      sb.append(String.valueOf(attributes.get(k)) + "\t");
    }
    sb.append(geometry.toString());
    return sb.toString();
  }

  public String toGeometryJson() throws IOException {
    GeometryJSON gjson = new GeometryJSON(9);
    return gjson.toString(this.geometry);
  }

  protected Map<String, Object> getGeometryMap() {
    Map<String, Object> geometryMap = new HashMap<>();
    geometryMap.put("type",this.geometry.getGeometryType());
    geometryMap.put("coordinates", this.geometry.getCoordinates());
    return geometryMap;
  }

  /**
   * Feature 对象转化为 GeoJson 字符串
   * @return
   * 点要素案例：
   * {"type":"Feature",
   *   "properties":{},
   *   "geometry":{
   *         "type":"Point",
   *         "coordinates":[105.380859375,31.57853542647338]
   *     }
   * }
   * TODO 待测
   * TODO 暂时还不知道怎么处理掉Z坐标，现在的方法很蠢
   */
  public String toJson() {
    Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    Map<String, Object> result = new HashMap<>();
    Map<String, Object> properties = this.attributes;
    properties.put("fid", this.fid);
    result.put("type", "Feature");
    result.put("properties", properties);
    result.put("geometry", this.getGeometryMap());
    String resultStr = gson.toJson(result);
    resultStr = resultStr.replace(",\"z\":NaN", "");
    return resultStr;
  }

}
