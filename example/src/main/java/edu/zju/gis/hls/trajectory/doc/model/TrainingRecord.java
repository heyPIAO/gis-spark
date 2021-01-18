package edu.zju.gis.hls.trajectory.doc.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.Row;
import scala.Serializable;

import java.sql.Date;

/**
 * @author Hu
 * @date 2021/1/9
 **/
@Getter
@Setter
@AllArgsConstructor
public class TrainingRecord implements Serializable {
  private Long index;
  private Double x1, x2;
  private Double y1, y2;
  private Long t1, t2;
  private Double scaledX;
  private Double scaledY;
  private Double scaledTime;
  private Double scaledIndex;
  private String trajId;

  public TrainingRecord(String line) {
    String[] fields = line.split(",");
    this.index = Long.valueOf(fields[0].trim());
    this.x1 = Double.valueOf(fields[1].trim());
    this.y1 = Double.valueOf(fields[2].trim());
    this.t1 = Long.valueOf(fields[3].trim());
    this.scaledX = Double.valueOf(fields[4].trim());
    this.scaledY = Double.valueOf(fields[5].trim());
    this.scaledTime = Double.valueOf(fields[6].trim());
    this.scaledIndex = Double.valueOf(fields[7].trim());
    this.trajId = fields[8].trim();
  }

  public TrainingRecord(Row row) {
    this.index = row.getAs("id");
    this.x1 = row.getAs("pickup_longitude");
    this.y1 = row.getAs("pickup_latitude");
    Date date1 = row.getAs("pickup_datetime");
    this.t1 = date1.getTime();
    this.x2 = row.getAs("dropoff_longitude");
    this.y2 = row.getAs("dropoff_latitude");
    Date date2 = row.getAs("dropoff_datetime");
    this.t2 = date2.getTime();
  }

  @Override
  public String toString() {
    return String.format("%d,%f,%f,%d,%f,%f,%d,%f,%f,%f,%f,%s",
      index, x1, y1, t1, x2, y2, t2, scaledX, scaledY, scaledTime, scaledIndex, trajId);
  }
}
