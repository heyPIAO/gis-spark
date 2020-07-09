package edu.zju.gis.hls.trajectory.analysis.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.WKTWriter2;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.AffineTransformation;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static edu.zju.gis.hls.trajectory.analysis.model.Term.GEOMETRY_JSON_DECIMAL;

/**
 * @author Hu
 * @date 2019/9/19
 * 封装Geometry，构筑含ID和属性信息的Feature
 **/
@Getter
@Setter
@NoArgsConstructor
//@AllArgsConstructor
public class Feature <T extends Geometry> implements Serializable {

  protected String fid;
  protected T geometry;
  protected LinkedHashMap<Field, Object> attributes; // HashMap的KeySet是乱序的，在数据输出时会导致顺序混乱，故使用 LinkedHashMap

  public Object getAttribute(String key){
    for (Map.Entry<Field, Object> a: attributes.entrySet()) {
      if (a.getKey().getName().equals(key)) {
        return a.getValue();
      }
    }
    return null;
  }

  public Feature(String fid, T geometry, LinkedHashMap<Field, Object> attributes) {
    this.fid = fid;
    this.geometry = geometry;
    this.attributes = attributes;
  }

  public Object getAttribute(Field key){
    return this.getAttribute(key.getName());
  }

  public Feature(Feature f){
    this(f.getFid(), (T)f.getGeometry(), f.getAttributes());
  }

  public Method getMethod(String name, Class<?>... parameterTypes) throws NoSuchMethodException {
    return this.getClass().getMethod(name, parameterTypes);
  }

  /**
   * 获取对象拷贝方法
   * @return
   */
  public Constructor getSelfCopyConstructor() throws NoSuchMethodException {
    return this.getClass().getConstructor(this.getClass());
  }

  public <F extends Feature> F getSelfCopyObject() {
    try {
      Constructor c = this.getSelfCopyConstructor();
      return (F) c.newInstance(this);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      e.printStackTrace();
    }
    return null;
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

    for(Field k: attributes.keySet()){
      if (k.isExist()) {
        sb.append(String.valueOf(attributes.get(k)) + "\t");
      }
    }
    sb.append(geometry.toString());
    return sb.toString();
  }

  public String getGeometryJson() throws IOException {
    GeometryJSON gjson = new GeometryJSON(GEOMETRY_JSON_DECIMAL);
    return gjson.toString(this.geometry);
  }

  public String getGeometryWkt() {
    WKTWriter2 wktWriter = new WKTWriter2(2);
    return wktWriter.write(geometry);
  }

  protected Map<String, Object> getGeometryMap() {
    Map<String, Object> geometryMap = new HashMap<>();
    geometryMap.put("type",this.geometry.getGeometryType());
    geometryMap.put("coordinates", this.geometry.getCoordinates());
    return geometryMap;
  }

  public LinkedHashMap<Field, Object> getExistAttributes() {
    LinkedHashMap<Field, Object> a = new LinkedHashMap<>();
    for (Field f: this.attributes.keySet()) {
      if (f.isExist()) {
        a.put(f, null);
      }
    }
    return a;
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
    Map<String, Object> properties = new HashMap<>();
    properties.put("fid", this.fid);
    for (Map.Entry<Field, Object> f: this.attributes.entrySet()) {
      if (f.getKey().isExist()) {
        properties.put(f.getKey().getName(), f.getValue().toString());
      }
    }
    result.put("type", "Feature");
    result.put("properties", properties);
    result.put("geometry", this.getGeometryMap());
    String resultStr = gson.toJson(result);
    resultStr = resultStr.replace(",\"z\":NaN", "");
    return resultStr;
  }

  public String[] toStringArray() {
    List<String> values = new LinkedList<>();
    values.add(this.fid);
    Set<Field> fields = this.getExistAttributes().keySet();
    for (Field f: fields) {
      values.add(String.valueOf(this.getAttribute(f)));
    }
    values.add(this.getGeometryWkt());
    String[] result = new String[values.size()];
    values.toArray(result);
    return result;
  }

  public Object[] toObjectArray() {
    List<Object> values = new LinkedList<>();
    values.add(this.fid);
    Set<Field> fields = this.getExistAttributes().keySet();
    for (Field f: fields) {
      values.add(this.getAttribute(f));
    }
    values.add(this.getGeometryWkt());
    Object[] result = new Object[values.size()];
    values.toArray(result);
    return result;
  }

}
