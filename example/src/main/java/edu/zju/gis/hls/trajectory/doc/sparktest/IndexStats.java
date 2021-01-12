package edu.zju.gis.hls.trajectory.doc.sparktest;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2021/1/12
 **/
@Getter
@Setter
@AllArgsConstructor
public class IndexStats implements Serializable {
  private Long featureSize;
  private Long buildTime;
  private Long indexSize;
}
