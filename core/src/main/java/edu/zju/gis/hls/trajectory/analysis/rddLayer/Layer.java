package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/
public abstract class Layer<K,V extends Feature> extends JavaPairRDD<K, V> implements Serializable {

  /**
   * key 为字段名
   * value 为字段中文名
   */
  @Getter
  protected Map<String, String> attributes;

  /**
   * key 为字段名
   * value 为字段类型
   */
  @Getter
  protected Map<String, String> attributeTypes;


  // TODO 用 field 代替 attributes
  @Getter
  protected Map<Integer, Field> fields;

  /**
   * 图层元数据信息
   */
  @Getter
  @Setter
  protected LayerMetadata metadata;

  protected Layer(){
    this(null, null, null);
  }

  protected Layer(RDD<Tuple2<K, V>> rdd, ClassTag<K> kClassTag, ClassTag<V> vClassTag) {
    super(rdd, kClassTag, vClassTag);
    this.metadata = new LayerMetadata();
    this.attributeTypes = new HashMap<>();
    this.attributes = new HashMap<>();
  }

  private <T extends Layer> T initialize(T layer, RDD<Tuple2<K, V>> rdd) {
    try {
      Constructor c = layer.getConstructor(RDD.class);
      T t = (T) c.newInstance(rdd);
      return copy(layer, t);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * only copy meta info of the layer
   * @return
   */
  protected <T extends Layer> T copy(T from, T to) {
    to.metadata = from.metadata;
    to.attributes = from.attributes;
    to.attributeTypes = from.attributeTypes;
    to.fields = from.fields;
    return to;
  }

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
   * 图层偏移
   * @param deltaX
   * @param deltaY
   * @return
   */
  public Layer shift(double deltaX, double deltaY) {

    Function shiftFunction = new Function<Tuple2<K, V>, Tuple2<K, V>>() {
      @Override
      public Tuple2<K, V> call(Tuple2<K, V> t) throws Exception {
        Feature f = t._2;
        f.shift(deltaX, deltaY);
        return new Tuple2<>(t._1, (V)f);
      }
    };

    Layer result = this.mapToLayer(shiftFunction);

    if (this.metadata.getExtent() != null) {
      this.metadata.shift(deltaX, deltaY);
    }

    return this.mapToLayer(shiftFunction);
  }

  public Constructor getConstructor(Class<?>... parameterTypes) throws NoSuchMethodException {
    return this.getClass().getConstructor(parameterTypes);
  }

  public Layer<K, V> flatMapToLayer(PairFlatMapFunction<Tuple2<K, V>, K, V> f) {
    return this.initialize(this, this.flatMapToPair(f).rdd());
  }

  public Layer<K, V> mapToLayer(Function<Tuple2<K, V>, Tuple2<K, V>> f) {
    return this.initialize(this, this.map(f).rdd());
  }

  public Layer<K, V> flatMapToLayer(FlatMapFunction<Tuple2<K, V>, Tuple2<K, V>> f) {
    return this.initialize(this, this.flatMap(f).rdd());
  }

}
