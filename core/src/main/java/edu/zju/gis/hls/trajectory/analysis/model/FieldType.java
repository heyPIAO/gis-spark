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
  START_TIME_FIELD("START_TIME"),
  END_TIME_FIELD("END_TIME"),
  NORMAL_FIELD("NORMAL_FIELD");

  @Getter
  @Setter
  private String fname;

  FieldType(String name) {
    this.fname = name;
  }



}
