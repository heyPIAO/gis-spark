package edu.zju.gis.hls.trajectory.analysis.proto;

import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2020/10/29
 **/
@Getter
@Setter
public class TRS {

  private String type;
  private Map<String, Object> properties;

  public static TRS getDefault() {
    TRS t = new TRS();
    t.type = "Name";
    Map<String, Object> p = new LinkedHashMap<>();
    p.put("name", "urn:ogc:data:time:iso8601");
    t.setProperties(p);
    return t;
  }

}
