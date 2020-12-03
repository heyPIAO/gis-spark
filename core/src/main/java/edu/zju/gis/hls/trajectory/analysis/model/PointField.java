package edu.zju.gis.hls.trajectory.analysis.model;

import static edu.zju.gis.hls.trajectory.analysis.model.FieldType.SHAPE_FIELD;

/**
 * @author Hu
 * @date 2020/10/3
 **/
public class PointField extends Field {

  public PointField() {
    super(SHAPE_FIELD.name(), org.locationtech.jts.geom.Point.class.getName(), FieldType.SHAPE_FIELD);
  }

}
