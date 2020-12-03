package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/12/19
 **/
public enum FieldType implements Serializable {
  ID_FIELD("FID"),
  SHAPE_FIELD("GEOM"),
  TIME_FIELD("TIME"),
  NORMAL_FIELD("NORMAL_FIELD"),
  TEMPORAL_FIELD("TEMPORAL_FIELD"); // 字段值随时间变化

  @Getter
  @Setter
  private String fname;

  FieldType(String name) {
    this.fname = name;
  }

}
