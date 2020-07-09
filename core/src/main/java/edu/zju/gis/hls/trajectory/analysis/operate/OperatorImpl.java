package edu.zju.gis.hls.trajectory.analysis.operate;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.SparkSession;

/**
 * @author Hu
 * @date 2020/7/9
 **/
@AllArgsConstructor
public abstract class OperatorImpl implements Operator {
  private SparkSession ss;
}
