package edu.zju.gis.hls.trajectory.datastore.storage.writer;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.StatLayer;
import lombok.Getter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-09-16
 */
public abstract class ImageWriter implements Serializable {
    @Getter
    transient protected SparkSession ss;
    @Getter transient protected JavaSparkContext jsc;

    public ImageWriter(SparkSession ss) {
        this.ss = ss;
        this.jsc = JavaSparkContext.fromSparkContext(this.ss.sparkContext());
    }

    public abstract void write(StatLayer imageLayer);
}
