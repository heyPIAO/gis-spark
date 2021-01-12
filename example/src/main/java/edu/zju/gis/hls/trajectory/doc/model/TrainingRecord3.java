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
public class TrainingRecord3 implements Serializable {
  private Long indexX;
  private Long indexY;
  private Long indexTime;
  private Double x;
  private Double y;
  private Long t;
  private Double scaledX;
  private Double scaledY;
  private Double scaledTime;
  private Double scaledIndexX;
  private Double scaledIndexY;
  private Double scaledIndexT;
  private String trajId;

  public TrainingRecord3(String line) {
    String[] fields = line.split(",");
    this.indexX = Long.valueOf(fields[0]);
    this.indexY = Long.valueOf(fields[1]);
    this.indexTime = Long.valueOf(fields[2]);
    this.x = Double.valueOf(fields[3]);
    this.y = Double.valueOf(fields[4]);
    this.t = Long.valueOf(fields[5]);
    this.scaledX = Double.valueOf(fields[6]);
    this.scaledY = Double.valueOf(fields[7]);
    this.scaledTime = Double.valueOf(fields[8]);
    this.scaledIndexX = Double.valueOf(fields[9]);
    this.scaledIndexY = Double.valueOf(fields[10]);
    this.scaledIndexT = Double.valueOf(fields[11]);
    this.trajId = fields[12];
  }

  @Override
  public String toString() {
    return String.format("%d,%d,%d,%f,%f,%d,%f,%f,%f,%f,%f,%f,%s",
      indexX, indexY, indexTime, x, y, t, scaledX, scaledY, scaledTime, scaledIndexX, scaledIndexY, scaledIndexT, trajId);
  }
}