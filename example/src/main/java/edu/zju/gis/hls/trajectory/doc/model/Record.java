package edu.zju.gis.hls.trajectory.doc.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import scala.Serializable;

/**
 * @author Hu
 * @date 2021/1/9
 **/
@Getter
@Setter
@AllArgsConstructor
public class Record implements Serializable {
  private Double x;
  private Double y;
  private Long time;
}