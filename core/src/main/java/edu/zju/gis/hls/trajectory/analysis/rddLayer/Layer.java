package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.datastore.base.FieldBasicStat;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Envelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2019/9/19
 **/
@Slf4j
public class Layer<K, V extends Feature> extends JavaPairRDD<K, V> implements Serializable {

    /**
     * 图层元数据信息
     */
    @Getter
    @Setter
    protected LayerMetadata metadata;

    private boolean isAnalyzed = false;

    /**
     * 推断图层属性字段元信息
     */
    public void inferFieldMetadata() {
        Feature f = this.first()._2;
        Iterator<Field> fields = f.getExistAttributes().keySet().iterator();
        List<Field> fo = new ArrayList<>();
        while (fields.hasNext()) fo.add(fields.next());
        Field[] foo = new Field[fo.size()];
        for (int i=0; i<foo.length; i++) {
            foo[i] = fo.get(i);
        }
        this.metadata.setAttributes(foo);
    }

    public static <F extends Feature> Layer<String, F> empty(JavaSparkContext jsc, Class<F> f) {
        List<Tuple2<String, F>> fs = new ArrayList<>();
        fs.add(new Tuple2<>(UUID.randomUUID().toString(), Feature.empty(f)));
        return new Layer<String, F>(jsc.parallelize(fs).rdd(),
                scala.reflect.ClassTag$.MODULE$.apply(String.class),
                scala.reflect.ClassTag$.MODULE$.apply(f));
    }

    public Layer(JavaSparkContext jsc, List<Tuple2<K,V>> features) {
        this(jsc.parallelize(features).mapToPair(x->new Tuple2<>(x._1, x._2)).rdd());
    }

    public Layer(RDD<Tuple2<K, V>> rdd) {
        super(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(Feature.class));
        this.metadata = new LayerMetadata();
    }

    public Layer(RDD<Tuple2<K, V>> rdd, ClassTag<K> kClassTag, ClassTag<V> vClassTag) {
        super(rdd, kClassTag, vClassTag);
        this.metadata = new LayerMetadata();
    }

    private Layer<K, V> initialize(Layer<K, V> layer, RDD<Tuple2<K, V>> rdd) {
        try {
            Constructor c = layer.getConstructor(RDD.class);
            Layer<K, V> t = (Layer<K, V>) c.newInstance(rdd);
            return copy(layer, t);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * only copy meta info of the layer
     *
     * @return
     */
    protected Layer<K, V> copy(Layer<K, V> from, Layer<K, V> to) {
        to.metadata = from.metadata;
        return to;
    }

    /**
     * 复制图层元数据信息
     *
     * @param from
     * @param <L>
     */
    public <L extends Layer> void copy(L from) {
        this.metadata = from.metadata;
    }


    public Field findAttribute(String name) {
        for (Field f : this.metadata.getAttributes().keySet()) {
            if (f.getName().equals(name)) {
                return f;
            }
        }
        return null;
    }

    /**
     * 图层偏移
     *
     * @param deltaX
     * @param deltaY
     * @return
     */
    public Layer<K, V> shift(double deltaX, double deltaY) {

        if (this.metadata.getExtent() != null) {
            this.metadata.shift(deltaX, deltaY);
        }

        Function shiftFunction = new Function<Tuple2<K, V>, Tuple2<K, V>>() {
            @Override
            public Tuple2<K, V> call(Tuple2<K, V> t) throws Exception {
                V f = (V)(new Feature(t._2));
                f.shift(deltaX, deltaY);
                return new Tuple2<>(t._1, f);
            }
        };

        return this.mapToLayer(shiftFunction);
    }

    public Layer<K,V> makeValid() {
        Function makeValidFunction = new Function<Tuple2<K, V>, Tuple2<K, V>>() {
            @Override
            public Tuple2<K, V> call(Tuple2<K, V> t) throws Exception {
                V f = (V)(new Feature(t._2));
                f.makeValid();
                return new Tuple2<>(t._1, f);
            }
        };
        return this.mapToLayer(makeValidFunction);
    }

    /**
     * 投影转换
     *
     * @param ocrs：目标crs
     * @return
     */
    public Layer<K, V> transform(CoordinateReferenceSystem ocrs) {
        return this.transform(metadata.getCrs(), ocrs);
    }

    public Layer<K, V> transform(CoordinateReferenceSystem srcCrs, CoordinateReferenceSystem ocrs) {
        Function transformFunction = (Function<Tuple2<K, V>, Tuple2<K, V>>) t -> {
            V f = (V)(new Feature(t._2));
            f.transform(srcCrs, ocrs);
            return new Tuple2<>(t._1, f);
        };
        this.metadata.setCrs(ocrs);
        return this.mapToLayer(transformFunction);
    }


    public Constructor getConstructor(Class<?>... parameterTypes) throws NoSuchMethodException {
        return this.getClass().getConstructor(parameterTypes);
    }

    /**
     * 图层类型不变的操作
     * 重写 RDD 的 map/flatMap/mapToPair/filter 等方法，继承图层的元数据信息
     * 通过强制类型转换，保留了图层的类型信息
     *
     * @param f
     * @return
     */
    public Layer<K, V> flatMapToLayer(PairFlatMapFunction<Tuple2<K, V>, K, V> f) {
        return this.initialize(this, this.flatMapToPair(f).rdd());
    }

    public Layer<K, V> mapToLayer(Function<Tuple2<K, V>, Tuple2<K, V>> f) {
        return this.initialize(this, this.map(f).rdd());
    }

    public Layer<K, V> reduceByKeyToLayer(Function2<V,V,V> f) {
        return this.initialize(this, this.reduceByKey(f).rdd());
    }

    public Layer<K, V> union(Layer<K, V> layer) {
        return this.initialize(this,this.rdd().union(layer.rdd()) );
    }

    public Layer<K, V> distinct() {
        return this.initialize(this,this.rdd().distinct() );
    }

    public Layer<K, V> mapToLayer(PairFunction<Tuple2<K, V>, K, V> f) {
        return this.initialize(this, this.mapToPair(f).rdd());
    }

    public Layer<K, V> flatMapToLayer(FlatMapFunction<Tuple2<K, V>, Tuple2<K, V>> f) {
        return this.initialize(this, this.flatMap(f).rdd());
    }

    public Layer<K, V> filterToLayer(Function<Tuple2<K, V>, Boolean> f) {
        return this.initialize(this, this.filter(f).rdd());
    }

    public Layer<K, V> repartitionToLayer(int num) {
        return this.initialize(this, this.repartition(num).rdd());
    }

    public Layer<K, V> repartitionToLayer() {
        return this.initialize(this, this.repartition(this.getNumPartitions()).rdd());
    }

    public Layer<K, V> mapPartitionsToLayer(PairFlatMapFunction f) {
        return this.initialize(this, this.mapPartitionsToPair(f).rdd());
    }

    public Layer<K, V> partitionByToLayer(Partitioner partitioner) {
        return this.initialize(this, this.partitionBy(partitioner).rdd());
    }

    public Layer<K, V> filterEmpty() {
        Function<Tuple2<K, V>, Boolean> filterFunction = new Function<Tuple2<K, V>, Boolean>() {
            @Override
            public Boolean call(Tuple2<K, V> v) throws Exception {
                return !(v._2).isEmpty();
            }
        };
        return this.filterToLayer(filterFunction);
    }

    /**
     * 如果图层未被缓存，则缓存
     * TODO 换个名字.. 太丑了..  @HLS
     *
     * @return
     */
    public void makeSureCached() {
        this.makeSureCached(StorageLevel.MEMORY_ONLY());
    }

    public void makeSureCached(StorageLevel level) {
        if (!isCached()) {
            this.persist(level);
        }
    }

    public void release() {
        if (isCached()) {
            this.unpersist();
        }
    }

    public boolean isCached() {
        return !this.getStorageLevel().equals(StorageLevel.NONE());
    }

    // TODO 默认的 distinct 效率太低了，先偷懒重写一下
    public List<String> distinctKeys() {
        this.makeSureCached();
        // 分区内部先去重
        JavaRDD<String> keys = this.keys().map(x -> (String) x).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterator<String> call(Iterator<String> it) {
                List<String> m = IteratorUtils.toList(it);
                List<String> r = m.stream().distinct().collect(Collectors.toList());
                return r.iterator();
            }
        });
        List<String> result = keys.collect();
        // collect 之后再去重
        return result.stream().distinct().collect(Collectors.toList());
    }

    /**
     * TODO 未测试
     * 分析当前图层
     * 获取信息包括：
     * （1）图层四至
     * （2）图层要素个数
     * （3）图层 Numeric 字段的最大/最小/均值（Long，Integer，Float等）
     * 分析结果存储于图层 LayerMetadata 中
     */
    public void analyze() {

        if (this.isAnalyzed) {
            log.info(String.format("Layer %s:%s has already analyzed",
                    this.metadata.getLayerId(), this.metadata.getLayerName()));
            return;
        }

        this.makeSureCached();

        JavaPairRDD<String, FieldBasicStat> basicStat = this.flatMapToPair(t -> {
            Feature f = (Feature) t._2;
            Map<Field, Object> attrs = f.getExistAttributes();
            List<Tuple2<String, FieldBasicStat>> result = new ArrayList<>();
            for (Field fd : attrs.keySet()) {
                if (fd.isNumeric() && fd.isExist()) {
                    FieldBasicStat stat = new FieldBasicStat();
                    Object num = attrs.get(fd.getName());
                    if (num != null) {
                        stat.setMin((Double) num);
                        stat.setMax((Double) num);
                        stat.setTotal((Double) num);
                        stat.setCount(1L);
                        result.add(new Tuple2<>(fd.getName(), stat));
                    }
                }
            }

            Envelope g = f.getGeometry().getEnvelopeInternal();
            FieldBasicStat xs = new FieldBasicStat();
            xs.setMin(g.getMinX());
            xs.setMax(g.getMaxX());
            xs.setCount(1L);
            result.add(new Tuple2<>("LONGITUDE", xs));

            FieldBasicStat ys = new FieldBasicStat();
            ys.setMin(g.getMinY());
            ys.setMax(g.getMaxY());
            result.add(new Tuple2<>("LATITUDE", ys));

            return result.iterator();
        });

        Map<String, FieldBasicStat> r = basicStat.reduceByKey(FieldBasicStat::add).collectAsMap();

        // 获取四至并构建图层的Geometry
        FieldBasicStat xs = r.get("LONGITUDE");
        FieldBasicStat ys = r.get("LATITUDE");
        this.metadata.setGeometry(JTS.toGeometry(new Envelope(xs.getMin(), xs.getMax(), ys.getMin(), ys.getMax())));
        this.metadata.setLayerCount(xs.getCount());
        for (String k : r.keySet()) {
            if (k.equals("LONGITUDE") || k.equals("LATITUDE")) continue;
            this.metadata.getAttributes().put(this.findAttribute(k), r.get(k));
        }

        this.isAnalyzed = true;
    }

    public StatLayer aggregateByField(String fieldName) {
        for (Field f: this.getMetadata().getAttributes().keySet()) {
            if (f.getName().equals(fieldName)) return this.aggregateByField(f);
        }
        throw new GISSparkException("no field named after " + fieldName);
    }

    public StatLayer aggregateByField(Field f) {
        if (f.isNumeric()) {
            return this.aggregateByField(f, (v1, v2)->((Number)v1).doubleValue()+((Number)v2).doubleValue());
        }
        throw new GISSparkException("Unvalid field for aggregator, default is set for numeric field");
    }

    /**
     * 自定义聚合函数
     * @param f
     * @return
     */
    public <F> StatLayer aggregateByField(Field f, BiFunction<F, F, F> aggregator) {
        JavaPairRDD<Field, Object> r = this.mapToPair(x->new Tuple2<>(f, x._2.getAttribute(f)));
        JavaRDD<Tuple2<String, LinkedHashMap<Field, Object>>> r2 = r.reduceByKey((v1,v2)->aggregator.apply((F)v1, (F)v2))
          .map(p ->{
              LinkedHashMap<Field, Object> m = new LinkedHashMap<>();
              m.put(p._1, p._2);
              return new Tuple2<>(p._1.getName(), m);
          });
        return new StatLayer(r2);
    }

    public void print() {
        this.collect().forEach(x -> log.info(x._2.toString()));
    }

    public boolean isSimpleLayer() {
        return !((this instanceof MultiPointLayer)
                || (this instanceof MultiPolylineLayer)
                || (this instanceof MultiPolygonLayer));
    }

    /**
     * Layer transform to Dataset
     */
    public Dataset<Row> toDataset(SparkSession ss) {
        JavaRDD<Row> rdd = this.rdd().toJavaRDD().map(x -> x._2).map(x -> new GenericRow(x.toObjectArray()));
        return ss.createDataFrame(rdd, this.getLayerStructType());
    }

    public Dataset<Row> toDataset(SparkSession ss, boolean geomReserved) {
        JavaRDD<Row> rdd = this.rdd().toJavaRDD()
                .map(x -> x._2)
                .map(x -> new GenericRow(x.toObjectArray(geomReserved)));
        return ss.createDataFrame(rdd, this.getLayerStructType(geomReserved));
    }

    /**
     * Layer transform to Dataset with GeometryField which is supported by user defined geometry function
     */
    public Dataset<Row> toGeomDataset(SparkSession ss) {
        JavaRDD<Row> rdd = this.rdd().toJavaRDD().map(x -> x._2).map(x -> new GenericRow(x.toObjectArray(true)));
        return ss.createDataFrame(rdd, this.getLayerStructType(true));
    }

    /**
     * 根据Layer的元数据信息获取 Dataset 的 StructType
     * StructType的顺序默认：ID为第1列，SHAPE为最后一列
     */
    private StructType getLayerStructType() {
        return this.getLayerStructType(false);
    }

    private StructType getLayerStructType(boolean geomReserved) {
        Map<Field, Object> layerAttrs = this.getMetadata().getExistAttributes();

        List<StructField> sfsl = new LinkedList<>();
        Field idField = this.getMetadata().getIdField();
        if (idField == null) {
            idField = Term.FIELD_DEFAULT_ID;
        }
        sfsl.add(convertFieldToStructField(idField));

        Field[] attrs = new Field[layerAttrs.size()];
        layerAttrs.keySet().toArray(attrs);
        for (int i = 0; i < layerAttrs.size(); i++) {
            if (attrs[i].getFieldType().equals(FieldType.ID_FIELD)
              || attrs[i].getFieldType().equals(FieldType.SHAPE_FIELD)) {
                continue;
            }
            sfsl.add(convertFieldToStructField(attrs[i]));
        }

        if (!(this instanceof StatLayer)) {
            Field shapeField = this.getMetadata().getShapeField();
            if (shapeField == null) {
                shapeField = Term.FIELD_DEFAULT_SHAPE;
            }
            sfsl.add(convertFieldToStructField(shapeField, geomReserved));
        }

        StructField[] sfs = new StructField[sfsl.size()];
        sfsl.toArray(sfs);
        return new StructType(sfs);
    }

    /**
     * 根据 Field 的元数据信息转成 StructField
     */
    private StructField convertFieldToStructField(Field field) {
        return new StructField(field.getName(),
                Field.converFieldTypeToDataType(field, false),
                !(field.getFieldType().equals(FieldType.ID_FIELD) || field.getFieldType().equals(FieldType.SHAPE_FIELD)),
                Metadata.empty());
    }

    private StructField convertFieldToStructField(Field field, boolean geomReserved) {
        return new StructField(field.getName(),
          Field.converFieldTypeToDataType(field, geomReserved),
          !(field.getFieldType().equals(FieldType.ID_FIELD) || field.getFieldType().equals(FieldType.SHAPE_FIELD)),
          Metadata.empty());
    }

}
