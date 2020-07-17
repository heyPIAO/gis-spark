package edu.zju.gis.hls.trajectory.datastore.base;

/**
 * @author Hu
 * @date 2020/7/8
 **/

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * 字段的基础统计值
 * FieldBasicStat的初始化值默认为0
 * TODO 初始化为0这样的设定不知道合不合适
 */
@Getter
@Setter
@ToString
public class FieldBasicStat implements Serializable {

  private Double min = Double.MAX_VALUE;
  private Double max = Double.MIN_VALUE;
  private Long count = 0L;
  private Double total = 0.0;

  public FieldBasicStat add(FieldBasicStat s) {
    FieldBasicStat result = new FieldBasicStat();
    result.min = Math.min(this.min, s.min);
    result.max = Math.max(this.max, s.max);
    result.count = this.count + s.count;
    result.total = this.total + s.total;
    return result;
  }

  public double getAvg() {
    return this.total/this.count;
  }

}
