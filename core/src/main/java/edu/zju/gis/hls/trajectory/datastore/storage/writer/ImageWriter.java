package edu.zju.gis.hls.trajectory.datastore.storage.writer;

import lombok.Getter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.Serializable;

/**
 * 图片写出抽象类
 * @author Keran Sun (katus)
 * @version 1.0, 2020-08-13
 */
public abstract class ImageWriter implements Serializable {
  @Getter transient protected SparkSession ss;
  @Getter transient protected JavaSparkContext jsc;

  public ImageWriter(SparkSession ss) {
    this.ss = ss;
    this.jsc = JavaSparkContext.fromSparkContext(this.ss.sparkContext());
  }

  public abstract void write(JavaPairRDD<String, BufferedImage> imageJavaPairRDD);
}
