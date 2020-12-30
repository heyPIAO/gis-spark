package edu.zju.gis.hls.trajectory.analysis.model;

import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 * 轨迹线：相较于普通的Polyline，多开始时间，结束时间两个属性
 **/
@Getter
@Setter
@Slf4j
public class MovingPoint extends Feature<TemporalLineString> {

  protected LinkedHashMap<Field, Object[]> temporalAttributes;

  public MovingPoint(String fid, TemporalLineString geometry, LinkedHashMap<Field, Object> attributes, LinkedHashMap<Field, Object[]> temporalAttributes) {
    super(fid, geometry, attributes);
    this.temporalAttributes = temporalAttributes;
  }

  public MovingPoint(MovingPoint f) {
    super(f);
    this.temporalAttributes = f.temporalAttributes;
    this.geometry = f.getGeometry();
  }

  private LinkedHashMap<Field, Object> getTemporalAttrIndic(int index) {
      LinkedHashMap<Field, Object> result = new LinkedHashMap<>();
      for (Field f: temporalAttributes.keySet()) {
          result.put(f, temporalAttributes.get(f)[index]);
      }
      return result;
  }

  public int getSize() {
      return Double.valueOf(this.geometry.getLength()).intValue();
  }

  public TimedPoint getIndic(int index) {
    LinkedHashMap<Field, Object> attrs = new LinkedHashMap<>(this.getAttributes());
    attrs.putAll(getTemporalAttrIndic(index));
    TemporalPoint tp = this.geometry.getTemporalPoint(index);
    String fid = this.getFid() + "_" + index;
    return new TimedPoint(fid, tp, attrs);
  }

  public long getTimeResolution() {
    return this.geometry.getTimeResolution();
  }


  public TrajectoryOD extractOD() {
    GeometryFactory gf = new GeometryFactory();
    LineString g = this.getGeometry();
    Coordinate[] coordinates = g.getCoordinates();
    if (coordinates.length < 2) {
      log.warn("Unvalid trajectory line: coordinates length smaller than 2");
      return null;
    }
    org.locationtech.jts.geom.Point start = gf.createPoint(coordinates[0]);
    org.locationtech.jts.geom.Point end = gf.createPoint(coordinates[coordinates.length-1]);

    // 构建temporalAttributes
    LinkedHashMap<Field, Object[]> temporalAttrsOD = new LinkedHashMap<>();
    for(Field f: temporalAttributes.keySet()) {
      Object[] os = temporalAttributes.get(f);
      temporalAttrsOD.put(f, new Object[]{os[0], os[os.length-1]});
    }

    return new TrajectoryOD(this.fid, start, end, this.getAttributes(), temporalAttrsOD, this.geometry.getInstants()[0], this.geometry.getInstants()[this.getSize()-1]);
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

      for (Field k: temporalAttributes.keySet()) {
          sb.append("[" + StringUtils.join(temporalAttributes.get(k), ",") + "]\t");
      }

      if (!this.isEmpty()) {
          sb.append(geometry.toString());
      } else {
          sb.deleteCharAt(sb.lastIndexOf("\t"));
      }
      return sb.toString();
  }

  public Object getAttribute(Field key){
    if (key.isTemporal()) return StringUtils.join(this.temporalAttributes.get(key), ",");
    return this.attributes.get(key);
  }

  @Override
  public LinkedHashMap<Field, Object> getExistAttributes() {
    LinkedHashMap<Field, Object> result = super.getExistAttributes();
    for (Field f: temporalAttributes.keySet()) {
        if (f.isExist()) result.put(f, null);
    }
    return result;
  }

  public boolean updateTemporalAttribute(String fname, int index, Object o) {
      if (this.getTemporalField(fname) == null) {
          log.warn(String.format("Field %s not exist, do nothing", fname));
          return false;
      }
      Field f = this.getTemporalField(fname);
      Object[] os = this.temporalAttributes.get(f);
      os[index] = o;
      this.temporalAttributes.put(f, os);
      return true;
  }

  public Field getTemporalField(Field key) {
    return this.getTemporalField(key.getName());
  }

  public Field getTemporalField(String name) {
    for (Map.Entry<Field, Object[]> a: temporalAttributes.entrySet()) {
        if (a.getKey().getName().equals(name)) {
            return a.getKey();
        }
    }
    return null;
  }

    @Override
  protected MovingPoint clone() {
    return new MovingPoint(this.fid, (TemporalLineString) this.geometry.copy(),
            (LinkedHashMap<Field, Object>) this.attributes.clone(),
            (LinkedHashMap<Field, Object[]>)this.temporalAttributes.clone());
  }


}
