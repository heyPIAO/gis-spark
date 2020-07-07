package edu.zju.gis.hls.trajectory.analysis.model;

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
  NORMA_FIELD("NORMAL_FIELD");

  private String name;

  FieldType(String name) {
    this.name = name;
  }

}
