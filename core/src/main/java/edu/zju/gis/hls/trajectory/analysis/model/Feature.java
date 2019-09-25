package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 * attributes 目前均为 String，不自动识别类型
 **/
@Getter
@Setter
@AllArgsConstructor
public abstract class Feature <T extends Geometry> implements Serializable {

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
   * 图层偏移
   */
  public abstract Feature shift(double deltaX, double deltaY);

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

  public abstract String toJson();

}
