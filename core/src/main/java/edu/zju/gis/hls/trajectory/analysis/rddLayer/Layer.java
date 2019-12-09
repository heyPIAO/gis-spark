package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.locationtech.jts.geom.util.AffineTransformation;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/
public abstract class Layer<K,V extends Feature> extends JavaPairRDD<K, V> implements Serializable {

  @Getter
  @Setter
  protected boolean hasIndexed;

  /**
   * key 为字段名
   * value 为字段中文名
   */
  @Getter
  protected Map<String, String> attributes;

  @Getter
  protected Map<String, String> attributeTypes;

  /**
   * 图层元数据信息
   */
  @Getter
  @Setter
  protected LayerMetadata metadata;

  protected Layer(){
    this(null, null, null);
  }

  private Layer(RDD<Tuple2<K, V>> rdd, ClassTag<K> kClassTag, ClassTag<V> vClassTag) {
    this(rdd, kClassTag, vClassTag, false);
  }

  public Layer(RDD<Tuple2<K, V>> rdd, ClassTag<K> kClassTag, ClassTag<V> vClassTag, boolean hasIndexed) {
    super(rdd, kClassTag, vClassTag);
    this.hasIndexed = hasIndexed;
    this.metadata = new LayerMetadata();
    this.attributeTypes = new HashMap<>();
    this.attributes = new HashMap<>();
  }

  public abstract Layer initialize(RDD<Tuple2<K, V>> rdd);

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
    this.attributes.put("fid", "FID");
    this.attributes.put("shape", "SHAPE");
  }

  public void setAttributeTypes(Map<String, String> attributeTypes) {
    this.attributeTypes = attributeTypes;
    this.attributeTypes.put("fid", String.class.getName());
    this.attributeTypes.put("shape", String.class.getName());
  }

  /**
   * 点偏移
   * @param deltaX
   * @param deltaY
   * @return
   */
  public Layer shift(double deltaX, double deltaY) {
    JavaRDD<Tuple2<K, V>> t = this.rdd().toJavaRDD();
    JavaRDD<Tuple2<K, V>> r = t.map(new Function<Tuple2<K, V>, Tuple2<K, V>>() {
      @Override
      public Tuple2<K, V> call(Tuple2<K, V> t) throws Exception {
        Feature f = t._2;
        f.shift(deltaX, deltaY);
        return new Tuple2<>(t._1, (V)f);
      }
    });
    return this.initialize(r.rdd());
  }

}
