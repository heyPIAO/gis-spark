package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/12/17
 * 图层字段
 **/
@Getter
@Setter
@AllArgsConstructor
public class Field implements Serializable {
  private String name;
  private String alias;
  private String type;
  private int length;
}
