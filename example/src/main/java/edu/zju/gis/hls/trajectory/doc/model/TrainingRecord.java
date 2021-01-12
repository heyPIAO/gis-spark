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
public class TrainingRecord implements Serializable {
  private Long index;
  private Double x;
  private Double y;
  private Long t;
  private Double scaledX;
  private Double scaledY;
  private Double scaledTime;
  private Double scaledIndex;
  private String trajId;

  public TrainingRecord(String line) {
    String[] fields = line.split(",");
    this.index = Long.valueOf(fields[0].trim());
    this.x = Double.valueOf(fields[1].trim());
    this.y = Double.valueOf(fields[2].trim());
    this.t = Long.valueOf(fields[3].trim());
    this.scaledX = Double.valueOf(fields[4].trim());
    this.scaledY = Double.valueOf(fields[5].trim());
    this.scaledTime = Double.valueOf(fields[6].trim());
    this.scaledIndex = Double.valueOf(fields[7].trim());
    this.trajId = fields[8].trim();
  }

  @Override
  public String toString() {
    return String.format("%d,%f,%f,%d,%f,%f,%f,%f,%s",
      index, x, y, t, scaledX, scaledY, scaledTime, scaledIndex, trajId);
  }
}
