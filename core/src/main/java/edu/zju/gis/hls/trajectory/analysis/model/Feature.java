package edu.zju.gis.hls.trajectory.analysis.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import edu.zju.gis.hls.trajectory.analysis.util.ClassUtil;
import edu.zju.gis.hls.trajectory.analysis.util.Converter;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.WKTWriter2;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.zju.gis.hls.trajectory.analysis.model.Term.GEOMETRY_JSON_DECIMAL;

/**
 * @author Hu
 * @date 2019/9/19
 * 封装Geometry，构筑含ID和属性信息的Feature
 **/
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class Feature <T extends Geometry> implements Serializable {

  protected String fid;
  protected T geometry;
  protected LinkedHashMap<Field, Object> attributes; // HashMap的KeySet是乱序的，在数据输出时会导致顺序混乱，故使用 LinkedHashMap


  public static Point empty() {
    return Feature.empty(Point.class);
  }

  public Feature(String fid, T geometry) {
    this(fid, geometry, new LinkedHashMap<>());
  }

  public Feature(T geometry, LinkedHashMap<Field, Object> attributes) {
    this(UUID.randomUUID().toString(), geometry, attributes);
  }

  public Feature(T geometry) {
    this(geometry, new LinkedHashMap<>());
  }

  /**
   * TODO is this safe to just set geometry = null ?
   * @param f
   * @return
   */
  public static <F extends Feature> F empty(Class<F> f) {
    try {
      Class gc = ClassUtil.getTClass(f, 0);
      Constructor c = f.getConstructor(String.class, gc, LinkedHashMap.class);
      return (F) c.newInstance(UUID.randomUUID().toString(), null, new LinkedHashMap<>());
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
      throw new GISSparkException("Unvalid feature type: " + f.getTypeName());
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      throw new GISSparkException("Unvalid feature type: " + f.getTypeName());
    } catch (InstantiationException e) {
      e.printStackTrace();
      throw new GISSparkException("Unvalid feature type: " + f.getTypeName());
    } catch (InvocationTargetException e) {
      e.printStackTrace();
      throw new GISSparkException("Unvalid feature type: " + f.getTypeName());
    }
  }

  public boolean isEmpty() {
    return this.geometry == null || this.geometry.isEmpty();
  }

  public Map<String, Object> getAttributesStr() {
    Map<String, Object> result = new LinkedHashMap<>();
    for (Field f: this.attributes.keySet()) {
      result.put(f.getName(), this.attributes.get(f));
    }
    return result;
  }

  public Object getAttribute(String key){
    for (Map.Entry<Field, Object> a: attributes.entrySet()) {
      if (a.getKey().getName().equals(key)) {
        return a.getValue();
      }
    }
    return null;
  }

  public Object getAttribute(Field key){
    return this.getAttribute(key.getName());
  }

  public Field getField(String name) {
    for (Map.Entry<Field, Object> a: attributes.entrySet()) {
      if (a.getKey().getName().equals(name)) {
        return a.getKey();
      }
    }
    return null;
  }

  public Feature(Feature f){
    LinkedHashMap<Field, Object> map = new LinkedHashMap<>();
    map.putAll(f.getAttributes());
    this.fid = f.getFid();
    this.attributes = map;
    if (f.isEmpty() && f.getGeometry() == null) {
      this.geometry = null;
    } else {
      this.geometry = (T)f.getGeometry().copy();
    }
  }

  public Feature intersect(Feature f, Boolean attrReserved) {
    Geometry g2 = f.geometry;
    if (f.getGeometry().isEmpty() || g2.isEmpty() || !this.getGeometry().intersects(g2)) return Feature.empty();
    Geometry g = this.getGeometry().intersection(g2);
    if (g.isEmpty()) {
      return Feature.empty();
    }
    Feature result = new Feature();
    result.setGeometry(g);
    result.setFid(this.getFid() + "_" + f.getFid());
    LinkedHashMap<Field, Object> attr = new LinkedHashMap<>();
    if (attrReserved) {
      for (Field i: this.getAttributes().keySet()) {
        Field io = new Field(i);
        io.setName(i.getName() + "_1");
        attr.put(io, this.getAttribute(i));
      }
      for (Object m: f.getAttributes().keySet()) {
        Field mf = (Field)m;
        Field io = new Field(mf);
        io.setName(mf.getName() + "_2");
        attr.put(io, f.getAttribute(mf));
      }
    }
    result.setAttributes(attr);
    return result;
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

  public void transform(CoordinateReferenceSystem ocrs, CoordinateReferenceSystem crs) throws FactoryException, TransformException {
    MathTransform mt = CRS.findMathTransform(ocrs, crs);
    this.geometry = (T) JTS.transform(this.geometry, mt);
  }

  public boolean isMulti() {
    return this.geometry instanceof  org.locationtech.jts.geom.MultiPoint
      || this.geometry instanceof  org.locationtech.jts.geom.MultiLineString
      || this.geometry instanceof org.locationtech.jts.geom.MultiPolygon;
  }

  public Feature buffer(double radius) {
    if (!this.isMulti()) {
      org.locationtech.jts.geom.Polygon p = (org.locationtech.jts.geom.Polygon) this.geometry.buffer(radius);
      return new Polygon(fid, p, attributes);
    } else {
      org.locationtech.jts.geom.MultiPolygon mp =
        (org.locationtech.jts.geom.MultiPolygon) Converter.convertToMulti(this.geometry.buffer(radius));
      return new MultiPolygon(fid, mp, attributes);
    }
  }

  /**
   * TODO 挪到 FeatureType 里面去，构建 Geometry 类型和 Feature 类型的映射关系
   * @param fid
   * @param g
   * @param attrs
   * @return
   */
  public static Feature buildFeature(String fid, Geometry g, LinkedHashMap<Field, Object> attrs) {
    if (g instanceof org.locationtech.jts.geom.Point) {
      return new Point(fid, (org.locationtech.jts.geom.Point)g, attrs);
    } else if (g instanceof org.locationtech.jts.geom.LineString) {
      return new Polyline(fid, (org.locationtech.jts.geom.LineString)g, attrs);
    } else if (g instanceof org.locationtech.jts.geom.Polygon) {
      return new Polygon(fid, (org.locationtech.jts.geom.Polygon)g, attrs);
    } else if (g instanceof org.locationtech.jts.geom.MultiPoint) {
      return new MultiPoint(fid, (org.locationtech.jts.geom.MultiPoint)g, attrs);
    } else if (g instanceof org.locationtech.jts.geom.MultiLineString) {
      return new MultiPolyline(fid, (org.locationtech.jts.geom.MultiLineString)g, attrs);
    } else if (g instanceof org.locationtech.jts.geom.MultiPolygon) {
      return new MultiPolygon(fid, (org.locationtech.jts.geom.MultiPolygon)g, attrs);
    } else {
      throw new GISSparkException("Unsupport geometry type for method: edu.zju.gis.hls.trajectory.analysis.model.Feature.buildFeature()");
    }
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
    if (!this.isEmpty()) {
      sb.append(geometry.toString());
    } else {
      sb.deleteCharAt(sb.lastIndexOf("\t"));
    }
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

  public void addAttribute(Field field, Object o) {
    this.attributes.put(field, o);
  }

  public void addAttributes(LinkedHashMap<Field, Object> o) {
    this.attributes.putAll(o);
  }

  public void addAttributes(LinkedHashMap<Field, Object> o, String prefix) {
    LinkedHashMap<Field, Object> of = new LinkedHashMap<>();
    for (Field f: o.keySet()) {
      Object v = of.get(f);
      f.setName(prefix + f.getName());
      of.put(f, v);
    }
    this.addAttributes(of);
  }

  public boolean updateAttribute(Field f, Object o) {
    return this.updateAttribute(f.getName(), o);
  }

  public boolean updateAttribute(String fname, Object o) {
    if (this.getAttribute(fname) == null) {
      log.warn(String.format("Field %s not exist, do nothing", fname));
      return false;
    }
    Field f = this.getField(fname);
    this.attributes.put(f, o);
    return true;
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
        //打印值？改为打印类型
        properties.put(f.getKey().getName(), f.getValue()!=null?f.getValue().toString():"NO_SET");
//        properties.put(f.getKey().getName(), f.getKey().getType());
      }
    }
    result.put("type", "Feature");
    result.put("properties", properties);
    if (!this.isEmpty()) {
      result.put("geometry", this.getGeometryMap());
    }
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
    if (!this.isEmpty()) {
      values.add(this.getGeometryWkt());
    }
    Object[] result = new Object[values.size()];
    values.toArray(result);
    return result;
  }

}
