package edu.zju.gis.hls.trajectory.doc.model;

import lombok.Getter;
import lombok.Setter;
import scala.Serializable;

/**
 * @author Hu
 * @date 2021/1/9
 **/
@Getter
@Setter
public class RecordStats implements Serializable {
  private Long count;
  private Double xmin;
  private Double ymin;
  private Double xmax;
  private Double ymax;
  private Long timeMin;
  private Long timeMax;

  public RecordStats(Long count, Double xmin, Double ymin, Double xmax, Double ymax, Long timeMin, Long timeMax) {
    this.count = count;
    this.xmin = xmin;
    this.ymin = ymin;
    this.xmax = xmax;
    this.ymax = ymax;
    this.timeMin = timeMin;
    this.timeMax = timeMax;
  }

  public Double xDiff() { return xmax - xmin; }
  public Double yDiff() { return ymax - ymin; }
  public Long timeDiff() { return timeMax - timeMin; }
}
