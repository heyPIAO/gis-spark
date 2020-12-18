package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.UniformGridIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.model.MultiPolygon;
import edu.zju.gis.hls.trajectory.analysis.model.Point;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.StatLayer;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.config.PgConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.helper.PgHelper;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.pg.PgLayerWriterConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.json.JSONObject;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

/**
 * @author Hu
 * @date 2019/11/12
 * 土地，二三调流量分析
 **/
@Getter
@Setter
@Slf4j
public class LandFlowAnalysis extends BaseModel<LandFlowAnalysisArgs> implements Serializable {

    private static Integer DEFAULT_INDEX_LEVEL = 8;
    private static Double DEFAULT_MU = 666.6666667;
    private static Double DEFAULT_GQ = 10000.00;//0921改为公顷
    private static String EMPTY_STRING = "_";
    private PgConfig pgConfig = new PgConfig();

    public LandFlowAnalysis(SparkSessionType type, String[] args) {
        super(type, args);
    }

    @Override
    protected void prepare() {
        super.prepare();
        this.initPgConfig();
    }

    private void initPgConfig() {
        // TODO 用正则取出来
        InputStream in = this.getClass().getResourceAsStream("/mysqlConfig.properties");
        Properties props = new Properties();
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(in, "UTF-8");
            props.load(inputStreamReader);
            this.pgConfig.setUrl((String) (props.get("pg.url")));
            this.pgConfig.setPort(Integer.valueOf(props.get("pg.port").toString()));
            this.pgConfig.setDatabase((String) props.get("pg.database"));
            this.pgConfig.setSchema((String) props.get("pg.schema"));
            this.pgConfig.setUsername((String) props.get("pg.username"));
            this.pgConfig.setPassword((String) props.get("pg.password"));
        } catch (IOException e) {
            throw new GISSparkException("read mysql configuration failed: " + e.getMessage());
        }
    }

    @Override
    public void run() throws Exception {
        StringBuilder logBuilder = new StringBuilder();
        PgHelper pgHelper = null;
        boolean ifSuccess = true;
        try {
            pgHelper = new PgHelper(this.pgConfig);
            LayerReaderConfig tb3dReaderConfig = LayerFactory.getReaderConfig(this.arg.getTb3dReaderConfig());
            LayerReader<MultiPolygonLayer> tb3dLayerReader = LayerFactory.getReader(ss, tb3dReaderConfig);
            LayerReaderConfig xz2dReaderConfig = LayerFactory.getReaderConfig(this.arg.getXz2dReaderConfig());
            MultiPolygonLayer xz2dLayer = (MultiPolygonLayer) this.read2dLayer(ss, xz2dReaderConfig).repartitionToLayer(ss.sparkContext().defaultParallelism());

            LayerWriterConfig statsWriterConfig = LayerFactory.getWriterConfig(this.arg.getStatsWriterConfig());
            JSONObject writeConfig = new JSONObject(this.arg.getStatsWriterConfig());
            //todo 暂时默认为pg输出,此外会出现taskname缺失，导致出错
            String taskName = this.arg.getTaskName();
            String tbName = writeConfig.getString("tablename");
            logBuilder.append("数据读取准备就绪...\r\n");
            pgHelper.runSQL(this.insertAnalysisInfo(), UUID.randomUUID()
                    , taskName
                    , "RUNNING"
                    , tbName
                    , this.arg.getStatsWriterConfig()
                    , logBuilder.toString()
                    , java.sql.Timestamp.from(Instant.now()));
//                    , java.sql.Date.from(Instant.now()));

            MultiPolygonLayer tb3dLayer = (MultiPolygonLayer) tb3dLayerReader.read().repartitionToLayer(ss.sparkContext().defaultParallelism());
            tb3dLayer.cache();

            JavaPairRDD<String, Double> testRDD = tb3dLayer.mapToPair(new PairFunction<Tuple2<String, MultiPolygon>, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Tuple2<String, MultiPolygon> input) throws Exception {
                    MultiPolygon polygon = new MultiPolygon(input._2);
                    Field field = polygon.getField("TBMJ");
                    Double tbmj = Double.valueOf(polygon.getAttribute(field).toString());
                    return new Tuple2<>("123", tbmj);
                }
            });
            Tuple2<String, Double> tuple2 = testRDD.reduce(new Function2<Tuple2<String, Double>, Tuple2<String, Double>, Tuple2<String, Double>>() {
                @Override
                public Tuple2<String, Double> call(Tuple2<String, Double> in1, Tuple2<String, Double> in2) throws Exception {
                    return new Tuple2<>(in1._1, in1._2 + in2._2);
                }
            });


            Double toArea = tuple2._2;
            BigDecimal bg = BigDecimal.valueOf(toArea / DEFAULT_MU);
            Double toAreaMU = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            BigDecimal bg1 = BigDecimal.valueOf(toArea / DEFAULT_GQ);
            Double toAreaGQ = bg1.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            String year = this.arg.getYear();
            String xzqdm = this.arg.getXzqdm();
            String xzqmc = this.arg.getXzqmc();
            pgHelper.runSQL(this.insertMJ(), UUID.randomUUID()
                    , xzqdm
                    , xzqmc
                    , year
                    , toArea
                    , toAreaGQ
                    , toAreaMU
                    , java.sql.Timestamp.from(Instant.now()));

//                    , java.sql.Date.from(Instant.now()));

            DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID, new UniformGridIndexConfig(DEFAULT_INDEX_LEVEL, false));

            KeyIndexedLayer<MultiPolygonLayer> l1 = si.index(xz2dLayer, false);
            KeyIndexedLayer<MultiPolygonLayer> l2 = si.index(tb3dLayer, false);

            MultiPolygonLayer connected2d = l1.getLayer(); // key 为 TileID
            MultiPolygonLayer tb3dLayerPair = l2.getLayer(); // key 为 TileID

            //test by cyr
            tb3dLayerPair.cache();

           // Map<String, MultiPolygon> ttt =  tb3dLayerPair.collectAsMap();
            List<Tuple2<String,MultiPolygon>> list= tb3dLayerPair.collect();
            int tttcount = list.size();
            //以上 by cyr

            logBuilder.append("数据读取完毕！计算中...\r\n");
            pgHelper.runSQL(this.updateAnalysisLog(taskName), logBuilder.toString());

            JavaPairRDD<String, Tuple2<MultiPolygon, MultiPolygon>> temp = tb3dLayerPair.cogroup(connected2d).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<MultiPolygon>, Iterable<MultiPolygon>>>, String, Tuple2<MultiPolygon, MultiPolygon>>() {
                @Override
                public Iterator<Tuple2<String, Tuple2<MultiPolygon, MultiPolygon>>> call(Tuple2<String, Tuple2<Iterable<MultiPolygon>, Iterable<MultiPolygon>>> in) throws Exception {
                    List<Tuple2<String, Tuple2<MultiPolygon, MultiPolygon>>> result = new ArrayList<>();
                    for (MultiPolygon tb3d : in._2._1) {
                        for (MultiPolygon tb2d : in._2._2) {
                            if (tb3d.getGeometry().intersects(tb2d.getGeometry())) {
                                //result.add(new Tuple2<>(tb3d.getFid()+ "##" + tb2d.getFid(), new Tuple2<>(tb3d, tb2d)));
                                Field field3d = tb3d.getField("BSM");
                                Field field2d = tb2d.getField("bsm");
                                result.add(new Tuple2<>(tb3d.getAttributes().get(field3d) + "##" + tb2d.getAttributes().get(field2d), new Tuple2<>(tb3d, tb2d)));
                            }
                        }
                    }
                    return result.iterator();
                }
            });

            //去重
            JavaPairRDD<String, Tuple2<MultiPolygon, MultiPolygon>> temp2 = temp.reduceByKey(
                    new Function2<Tuple2<MultiPolygon, MultiPolygon>, Tuple2<MultiPolygon, MultiPolygon>, Tuple2<MultiPolygon, MultiPolygon>>() {
                        @Override
                        public Tuple2<MultiPolygon, MultiPolygon> call(Tuple2<MultiPolygon, MultiPolygon> polygonFeatureMultiPolygonTuple2, Tuple2<MultiPolygon, MultiPolygon> polygonFeatureMultiPolygonTuple22) throws Exception {
                            return polygonFeatureMultiPolygonTuple2;
                        }
                    }
            );

            JavaPairRDD<String, Tuple2<MultiPolygon, Tuple3<Feature, Feature[], Feature[]>>> joined = temp2.mapToPair(new PairFunction<Tuple2<String, Tuple2<MultiPolygon, MultiPolygon>>, String, Tuple2<MultiPolygon, Tuple3<Feature, Feature[], Feature[]>>>() {
                @Override
                public Tuple2<String, Tuple2<MultiPolygon, Tuple3<Feature, Feature[], Feature[]>>> call(Tuple2<String, Tuple2<MultiPolygon, MultiPolygon>> in) throws Exception {
                    Feature[] lx2d = (Feature[]) in._2._2.getAttributesStr().get("lxdw");
                    Feature[] xz2d = (Feature[]) in._2._2.getAttributesStr().get("xzdw");
                    return new Tuple2<>(in._1, new Tuple2<>(in._2._1, new Tuple3<>(in._2._2, lx2d, xz2d)));
                }
            });

            JavaPairRDD<String, Geometry> intersected = joined.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<MultiPolygon, Tuple3<Feature, Feature[], Feature[]>>>, String, Geometry>() {
                @Override
                public Iterator<Tuple2<String, Geometry>> call(Tuple2<String, Tuple2<MultiPolygon, Tuple3<Feature, Feature[], Feature[]>>> t) throws Exception {

                    List<Tuple2<String, Geometry>> result = new ArrayList<>();

                    MultiPolygon tb3d = t._2._1;
                    MultiPolygon tb2d = (MultiPolygon) t._2._2._1();
                    Feature feature = new Feature();
                    Geometry tb3dg = feature.validedGeom((org.locationtech.jts.geom.MultiPolygon) tb3d.getGeometry());
                    Geometry tb2dg = feature.validedGeom((org.locationtech.jts.geom.MultiPolygon) tb2d.getGeometry());
                    tb3d.setGeometry((org.locationtech.jts.geom.MultiPolygon) tb3dg);
                    tb2d.setGeometry((org.locationtech.jts.geom.MultiPolygon) tb2dg);
//                tb3d = polygonFeatureEx(tb3d);
//                tb2d = polygonFeatureEx(tb2d);

                    try {
                        if (!tb3d.getGeometry().intersects(tb2d.getGeometry())) {
                            return result.iterator();
                        }

                        //test by cyr
                        if(tb3d.getAttribute("BSM").equals("330822211000084435")) {
                            if (tb2d.getAttribute("bsm").equals("215667")) {
                                log.info("123");
                            }
                        }
                        //以上 by cyr

                        Point[] lx2d = (Point[]) t._2._2._2();
                        MultiPolyline[] xz2d = (MultiPolyline[]) t._2._2._3();

                        String fcc = String.valueOf(tb3d.getAttribute("DLBM"));
                        String zldwdm = String.valueOf(tb3d.getAttribute("ZLDWDM")).substring(0, 6);//县级
                        String dlbz = String.valueOf(tb2d.getAttribute("dlbz"));
                        if(dlbz==null||dlbz.equals("")||dlbz.equals("null")){
                            dlbz = EMPTY_STRING;
                        }
                        String zzsxdm = String.valueOf(tb3d.getAttribute("ZZSXDM"));
                        if(zzsxdm==null||zzsxdm.equals("")||zzsxdm.equals("null")){
                            zzsxdm = EMPTY_STRING;
                        }

                        //和 2调面状图斑 进行相交
                        String omzcc = String.valueOf(tb2d.getAttribute("dlbm"));
                        double tbArea = Double.valueOf(String.valueOf(tb3d.getAttribute("TBMJ")));//0921取三调面积
                        double aarea = tb3d.getGeometry().getArea();//0921取三调面积

                        Geometry ip = tb3d.getGeometry().intersection(tb2d.getGeometry());
                        int size = ip.getNumGeometries();
                        a:
                        for (int i = 0; i < size; i++) {//为什么要分开
                            Geometry g = ip.getGeometryN(i);
                            if (g.getDimension() < 2) continue;
                            double iarea = g.getArea();
                            double flowTbArea = tbArea * iarea / aarea;

                            double sdkcxs = Double.valueOf(String.valueOf(tb3d.getAttribute("KCXS")));
                            double sdkcarea = flowTbArea * sdkcxs;

                            //耕地互变

                            if (omzcc.substring(0, 2).equals("01") && (fcc.substring(0, 2).equals("01")) &&
                                    (!omzcc.substring(omzcc.length() - 1).equals(fcc.substring(fcc.length() - 1)))) {
                                List<String> dlbms = new ArrayList<>();
                                List<Double> areas = new ArrayList<>();
                                List<String> dlbzs = new ArrayList<>();

                                //线状计算
                                if (xz2d != null) {
                                    for (MultiPolyline lf : xz2d) {
                                        if (!g.intersects(lf.getGeometry())) continue;
                                        String oxzcc = String.valueOf(lf.getAttribute("dlbm"));
                                        String xz_dlbz = String.valueOf(lf.getAttribute("dlbz"));
                                        if(xz_dlbz==null||xz_dlbz.equals("")||xz_dlbz.equals("null")||xz_dlbz.equals("NULL")){
                                            xz_dlbz = EMPTY_STRING;
                                        }
                                        try {
                                            Geometry ig = lf.getGeometry().intersection(g);
                                            double xzratio = ig.getLength() / lf.getGeometry().getLength();
                                            double oxzarea = xzratio * Double.valueOf(String.valueOf(lf.getAttribute("cd"))) * Double.valueOf(String.valueOf(lf.getAttribute("kd")));
                                            if (!oxzcc.equals(fcc)) {
                                                double okcbl = Double.valueOf(String.valueOf(lf.getAttribute("kcbl")));
                                                oxzarea = oxzarea * okcbl;
                                                dlbms.add(oxzcc);
                                                areas.add(oxzarea);
                                                dlbzs.add(xz_dlbz);

                                                flowTbArea = flowTbArea - oxzarea;
                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                            log.info(e.getLocalizedMessage());
                                            String error = String.format("Intersection Error: 3D(%s) vs 2D(%s), abort", tb3d.toString(), lf.toString());
                                            log.info("线状计算出错！");
                                            log.info(error);
                                            logBuilder.append(error + "\r\n");
                                            throw new GISSparkException(e.getLocalizedMessage());
                                        }
                                    }
                                }

                                //点状地物进行计算
                                if (lx2d != null) {
                                    for (Point pf : lx2d) {
                                        if (!g.contains(pf.getGeometry())) continue;
                                        String lx_dlbz = String.valueOf(pf.getAttribute("dlbz"));
                                        if(lx_dlbz==null||lx_dlbz.equals("")||lx_dlbz.equals("null")||lx_dlbz.equals("NULL")){
                                            lx_dlbz = EMPTY_STRING;
                                        }
                                        String olxcc = String.valueOf(pf.getAttribute("dlbm"));
                                        double olxarea = (double) pf.getAttribute("mj");
                                        if (!olxcc.equals(fcc)) {
                                            dlbms.add(olxcc);
                                            areas.add(olxarea);
                                            dlbzs.add(lx_dlbz);
                                            flowTbArea = flowTbArea - olxarea;
                                        }
                                    }
                                }

                                double tkxs = Double.valueOf(String.valueOf(tb2d.getAttribute("tkxs")));
                                double edkcarea;
                                if (tkxs < 1) {
                                    edkcarea = flowTbArea * tkxs;
                                } else {
                                    edkcarea = flowTbArea * tkxs * 0.01;
                                }

                                if (edkcarea <= sdkcarea) {
                                    //相当于非耕地到耕地
                                    flowTbArea = tbArea * iarea / aarea;
                                    flowTbArea = flowTbArea - sdkcarea + edkcarea;
                                    double kcarea = sdkcarea - edkcarea;
                                    String kc2 = String.format("123##1203##%s##%.9f##%s##%s",  zldwdm, sdkcarea - edkcarea,EMPTY_STRING,EMPTY_STRING );
                                    Tuple2<String, Geometry> flow2 = new Tuple2<>(kc2, null);
                                    result.add(flow2);
                                    for (int j = 0; j < dlbms.size(); j++) {
                                        String occ = dlbms.get(j);
                                        double area = areas.get(j);
                                        String odlbz = dlbzs.get(j);
//                                        if (flowTbArea - area < 0) {
//                                            area = flowTbArea;
//                                        }
                                        if (kcarea > 0) {

                                            //occ ## fcc ## ozldwdm ## fzldwdm ## area
                                            String kc = String.format("%s##1203##%s##%.9f##%s##%s", occ, zldwdm, area * sdkcxs,EMPTY_STRING,odlbz );
                                            Tuple2<String, Geometry> flow = new Tuple2<>(kc, null);
                                            result.add(flow);

                                            String k = String.format("%s##%s##%s##%.9f##%s##%s", occ, fcc, zldwdm, area - area * sdkcxs, zzsxdm,odlbz);
                                            Tuple2<String, Geometry> flowk = new Tuple2<>(k, null);
                                            result.add(flowk);


                                            kcarea = kcarea - area * sdkcxs;
                                        } else {
                                            String k = String.format("%s##%s##%s##%.9f##%s##%s", occ, fcc, zldwdm, area, zzsxdm,odlbz);
                                            Tuple2<String, Geometry> flow = new Tuple2<>(k, null);
                                            result.add(flow);
                                        }
                                        flowTbArea = flowTbArea - area;
//                                        if (flowTbArea < 0) {
//                                            continue a;
//                                        }
                                    }

                                    //面状地物情况
                                    if (kcarea > 0) {
                                        String kc = String.format("%s##1203##%s##%.9f##%s##%s", omzcc, zldwdm, kcarea, EMPTY_STRING,dlbz);
                                        Tuple2<String, Geometry> flow = new Tuple2<>(kc, null);
                                        result.add(flow);

                                        String k = String.format("%s##%s##%s##%.9f##%s##%s", omzcc, fcc, zldwdm, flowTbArea - kcarea-edkcarea, zzsxdm,dlbz);
                                        Tuple2<String, Geometry> flowk = new Tuple2<>(k, g);
                                        result.add(flowk);
                                    } else {
                                        String k = String.format("%s##%s##%s##%.9f##%s##%s", omzcc, fcc, zldwdm, flowTbArea, zzsxdm,dlbz);
                                        Tuple2<String, Geometry> flow = new Tuple2<>(k, g);
                                        result.add(flow);
                                    }


                                } else {
                                    //相当于耕地到非耕地
                                    flowTbArea = tbArea * iarea / aarea;
                                    double kcarea = edkcarea - sdkcarea;
                                    String k2 = String.format("123##1203##%s##%.9f##%s##%s",  zldwdm, edkcarea - sdkcarea, EMPTY_STRING ,EMPTY_STRING);
                                    Tuple2<String, Geometry> flow2 = new Tuple2<>(k2, null);
                                    result.add(flow2);
                                    for (int j = 0; j < dlbms.size(); j++) {
                                        String occ = dlbms.get(j);
                                        double area = areas.get(j);
                                        String odlbz = dlbzs.get(j);
                                        if (flowTbArea - area < 0) {
                                            area = flowTbArea;
                                        }
                                        String k = String.format("%s##%s##%s##%.9f##%s##%s", occ, fcc, zldwdm, area, zzsxdm,odlbz);
                                        Tuple2<String, Geometry> flow = new Tuple2<>(k, null);
                                        result.add(flow);

                                        flowTbArea = flowTbArea - area;
                                        if (flowTbArea < 0) {
                                            continue a;
                                        }
                                    }

                                    //面状地物
                                    if (flowTbArea - kcarea < 0) {
                                        kcarea = flowTbArea;
                                    }
                                    String kc = String.format("123##%s##%s##%.9f##%s##%s", fcc, zldwdm, kcarea, zzsxdm, EMPTY_STRING);
                                    Tuple2<String, Geometry> flowkc = new Tuple2<>(kc, g);
                                    result.add(flowkc);

                                    //防止出现流转完田坎后面积用尽的特殊情况
                                    flowTbArea = flowTbArea - kcarea;
                                    if (flowTbArea < 0) {
                                        continue a;
                                    }

                                    String k = String.format("%s##%s##%s##%.9f##%s##%s", omzcc, fcc, zldwdm, flowTbArea, zzsxdm, dlbz);
                                    Tuple2<String, Geometry> flow = new Tuple2<>(k, g);
                                    result.add(flow);
                                }

                            } else {
                                //和线状图斑进行流转
                                if (xz2d != null) {
                                    for (MultiPolyline lf : xz2d) {
                                        if (!g.intersects(lf.getGeometry())) continue;
                                        String oxzcc = String.valueOf(lf.getAttribute("dlbm"));
                                        try {
                                            Geometry ig = lf.getGeometry().intersection(g);
                                            double xzratio = ig.getLength() / lf.getGeometry().getLength();
                                            double oxzarea = xzratio * Double.valueOf(String.valueOf(lf.getAttribute("cd"))) * Double.valueOf(String.valueOf(lf.getAttribute("kd")));
                                            if (!oxzcc.equals(fcc)) {
                                                double okcbl = Double.valueOf(String.valueOf(lf.getAttribute("kcbl")));
                                                String xz_dlbz = String.valueOf(lf.getAttribute("dlbz"));
                                                if(xz_dlbz==null||xz_dlbz.equals("")||xz_dlbz.equals("null")){
                                                    xz_dlbz = EMPTY_STRING;
                                                }
                                                oxzarea = oxzarea * okcbl;
//                                                if (flowTbArea - oxzarea < 0) {
//                                                    oxzarea = flowTbArea;
//                                                }
                                                if (sdkcarea > 0) {
                                                    String kc = String.format("%s##1203##%s##%.9f##%s##%s", oxzcc, zldwdm, oxzarea * sdkcxs, EMPTY_STRING, xz_dlbz);
                                                    Tuple2<String, Geometry> flow = new Tuple2<>(kc, ig);
                                                    result.add(flow);

                                                    String k = String.format("%s##%s##%s##%.9f##%s##%s", oxzcc, fcc, zldwdm, oxzarea - oxzarea * sdkcxs, zzsxdm,xz_dlbz);
                                                    Tuple2<String, Geometry> flowk = new Tuple2<>(k, ig);
                                                    result.add(flowk);

                                                    sdkcarea = sdkcarea - oxzarea * sdkcxs;
                                                } else {
                                                    String k = String.format("%s##%s##%s##%.9f##%s##%s", oxzcc, fcc, zldwdm, oxzarea, zzsxdm, xz_dlbz);
                                                    Tuple2<String, Geometry> flow = new Tuple2<>(k, ig);
                                                    result.add(flow);
                                                }

                                                flowTbArea = flowTbArea - oxzarea;
//                                                if (flowTbArea < 0) {
//                                                    continue a;
//                                                }
                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                            log.info(e.getLocalizedMessage());
                                            String error = String.format("Intersection Error: 3D(%s) vs 2D(%s), abort", tb3d.toString(), lf.toString());
                                            log.info("线状计算出错！");
                                            log.info(error);
                                            logBuilder.append(error + "\r\n");
                                        }
                                    }
                                }

                                //和点状地物进行流转
                                if (lx2d != null) {
                                    for (Point pf : lx2d) {
                                        if (!g.contains(pf.getGeometry())) continue;
                                        String olxcc = String.valueOf(pf.getAttribute("dlbm"));
                                        double olxarea = (double) pf.getAttribute("mj");
                                        String lx_dlbz = String.valueOf(pf.getAttribute("dlbz"));
                                        if(lx_dlbz==null||lx_dlbz.equals("")||lx_dlbz.equals("null")){
                                            lx_dlbz = EMPTY_STRING;
                                        }
//                                        if (flowTbArea - olxarea < 0) {
//                                            olxarea = flowTbArea;
//                                        }
                                        if (!olxcc.equals(fcc)) {
                                            if (sdkcarea > 0) {
                                                String kc = String.format("%s##1203##%s##%.9f##%s##%s", olxcc, zldwdm, olxarea * sdkcxs, EMPTY_STRING, lx_dlbz);
                                                Tuple2<String, Geometry> flow = new Tuple2<>(kc, pf.getGeometry());
                                                result.add(flow);

                                                String k = String.format("%s##%s##%s##%.9f##%s##%s", olxcc, fcc, zldwdm, olxarea - olxarea * sdkcxs, zzsxdm, lx_dlbz);
                                                Tuple2<String, Geometry> flowk = new Tuple2<>(k, pf.getGeometry());
                                                result.add(flowk);

                                                sdkcarea = sdkcarea - olxarea * sdkcxs;
                                            } else {
                                                String k = String.format("%s##%s##%s##%.9f##%s##%s", olxcc, fcc, zldwdm, olxarea, zzsxdm, lx_dlbz);
                                                Tuple2<String, Geometry> flow = new Tuple2<>(k, pf.getGeometry());
                                                result.add(flow);
                                            }
                                        }

                                        flowTbArea = flowTbArea - olxarea;
//                                        if (flowTbArea < 0) {
//                                            continue a;
//                                        }
                                    }
                                }

                                //和面状进行流转
                                if (!omzcc.equals(fcc)) {
                                    //二调耕地到三调非耕地的情况
                                    if (omzcc.substring(0, 2).equals("01") && (!fcc.substring(0, 2).equals("01"))) {
                                        double tkxs = Double.valueOf(String.valueOf(tb2d.getAttribute("tkxs")));
                                        double kcarea;
                                        if (tkxs < 1) {
                                            kcarea = flowTbArea * tkxs;
                                        } else {
                                            kcarea = flowTbArea * tkxs * 0.01;
                                        }
                                        String kc = String.format("123##%s##%s##%.9f##%s##%s", fcc, zldwdm, kcarea, zzsxdm, EMPTY_STRING);
                                        Tuple2<String, Geometry> flowkc = new Tuple2<>(kc, g);
                                        result.add(flowkc);

                                        String k = String.format("%s##%s##%s##%.9f##%s##%s", omzcc, fcc, zldwdm, flowTbArea - kcarea, zzsxdm, dlbz);
                                        Tuple2<String, Geometry> flow = new Tuple2<>(k, g);
                                        result.add(flow);

                                    } else {
                                        //二调非耕地到三调耕地
                                        if (sdkcarea > 0) {
                                            String kc = String.format("%s##1203##%s##%.9f##%s##%s", omzcc, zldwdm, sdkcarea, EMPTY_STRING, dlbz);
                                            Tuple2<String, Geometry> flow = new Tuple2<>(kc, g);
                                            result.add(flow);

                                            String k = String.format("%s##%s##%s##%.9f##%s##%s", omzcc, fcc, zldwdm, flowTbArea - sdkcarea, zzsxdm, dlbz);
                                            Tuple2<String, Geometry> flowk = new Tuple2<>(k, g);
                                            result.add(flowk);
                                        } else {
                                            //二调非耕地到三调非耕地
                                            String k = String.format("%s##%s##%s##%.9f##%s##%s", omzcc, fcc, zldwdm, flowTbArea, zzsxdm, dlbz);
                                            Tuple2<String, Geometry> flow = new Tuple2<>(k, g);
                                            result.add(flow);
                                        }
                                    }

                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.info(e.getLocalizedMessage());
                        log.info(tb3d.toString());
                        log.info(tb2d.toString());
                        // String error = String.format("Intersection Error: 3D(%s) vs 2D(%s), abort", tb3d.getAttribute("BSM"), tb2d.getFid());
                        String error = e.getCause().toString();
                        log.info("面状计算出错！");
                        log.info(error);
                        logBuilder.append(error + "\r\n");
                        throw new GISSparkException(e.getLocalizedMessage());
                    }

                    return result.iterator();
                }
            });
            pgHelper.runSQL(this.updateAnalysisLog(taskName), logBuilder.toString());

//            Layer intersectedLayer = new Layer(intersected.map(new Function<Tuple2<String, Geometry>, Tuple2<String, Feature>>() {
//                @Override
//                public Tuple2<String, Feature> call(Tuple2<String, Geometry> in) throws Exception {
//                    return new Tuple2<>(in._1, new Feature(in._2));
//                }
//            }).rdd());
//
//            LayerWriterConfig geomWriterConfig = LayerFactory.getWriterConfig(this.arg.getGeomWriterConfig());
//            LayerWriter geomWriter = LayerFactory.getWriter(this.ss, geomWriterConfig);
//            intersectedLayer.inferFieldMetadata();
//            geomWriter.write(intersectedLayer);

            //geometry输出到本地txt
//            intersected.cache();
//            String fileName = this.arg.getGeomWriterConfig();
//            FileWriter writer = new FileWriter(fileName);
//
//            Map<String, Geometry> interMap = intersected.collectAsMap();
//            for (String s : interMap.keySet()) {
//                if(interMap.get(s)==null){
//                    writer.write(s + "##" + "" + "##" + "\n");
//                }else{
//                    writer.write(s + "##" + interMap.get(s).toText() + "##" + "\n");
//                }
//
//            }
//            writer.close();



            // 写出统计结果
            JavaPairRDD<String, Double> flowAreaRDD = intersected.mapToPair(new PairFunction<Tuple2<String, Geometry>, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Tuple2<String, Geometry> in) throws Exception {
                    String[] fs = in._1.split("##");

                    String key = String.format("%s##%s##%s##%s##%s", fs[0], fs[1], fs[2], fs[4], fs[5]);
                    Double v = Double.valueOf(fs[3]);
                    return new Tuple2<>(key, v);
                }
            }).reduceByKey((x1, x2) -> x1 + x2);

            flowAreaRDD.cache();





            JavaRDD<Tuple2<String, LinkedHashMap<Field, Object>>> statResult = flowAreaRDD.map(x -> {
                LinkedHashMap<Field, Object> r = new LinkedHashMap<>();
                Field f = new Field("mj");
                f.setType(java.lang.Double.class);
                String[] fs = x._1.split("##");
                if(fs[3].equals(EMPTY_STRING)||fs[3]==null||fs[3].equals("null")){
                    fs[3] = "";
                }
                if(fs[4].equals(EMPTY_STRING)||fs[4]==null||fs[4].equals("null")){
                    fs[4] = "";
                }
                Field f2 = new Field("qdlbm");
                f2.setType(java.lang.String.class);
                Field f3 = new Field("hdlbm");
                f3.setType(java.lang.String.class);
                Field f4 = new Field("zldwdm");
                f4.setType(java.lang.String.class);
                Field f5 = new Field("dlbz");
                f5.setType(java.lang.String.class);
                Field f6 = new Field("zzsxdm");
                f6.setType(java.lang.String.class);
                r.put(f, x._2);
                r.put(f2, fs[0]);
                r.put(f3, fs[1]);
                r.put(f4, fs[2]);
                r.put(f5, fs[4]);
                r.put(f6, fs[3]);
                return new Tuple2<>(x._1, r);
            });

            StatLayer statLayer = new StatLayer(statResult);
            Field f = new Field("mj");
            f.setType(Double.class);
            statLayer.getMetadata().addAttribute(f, null);
            Field f2 = new Field("qdlbm");
            f2.setType(String.class);
            statLayer.getMetadata().addAttribute(f2, null);
            Field f3 = new Field("hdlbm");
            f3.setType(String.class);
            statLayer.getMetadata().addAttribute(f3, null);
            Field f4 = new Field("zldwdm");
            f4.setType(String.class);
            statLayer.getMetadata().addAttribute(f4, null);
            Field f5 = new Field("dlbz");
            f5.setType(String.class);
            statLayer.getMetadata().addAttribute(f5, null);
            Field f6 = new Field("zzsxdm");
            f6.setType(String.class);
            statLayer.getMetadata().addAttribute(f6, null);







            JavaPairRDD<String, Double> flowAreaRDDK = flowAreaRDD.mapToPair(new PairFunction<Tuple2<String, Double>, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Tuple2<String, Double> in) throws Exception {
                    String[] fs = in._1.split("##");
                    if(fs[4].equals(EMPTY_STRING)||fs[4]==null||fs[4].equals("null")){
                        fs[4] ="";
                    }
                    String dlbm = fs[0];
                    if(fs[4].equals("K")||fs[4].equals("k")){
                        dlbm = dlbm + "k";
                    }
                    String key = String.format("%s##%s##%s", dlbm, fs[1], fs[2]);
                    Double v = in._2;
                    return new Tuple2<>(key, v);
                }
            }).reduceByKey((x1, x2) -> x1 + x2);
            JavaRDD<Tuple2<String, LinkedHashMap<Field, Object>>> statResultK = flowAreaRDDK.map(x -> {
                LinkedHashMap<Field, Object> r = new LinkedHashMap<>();
                Field fk = new Field("mj");
                fk.setType(java.lang.Double.class);
                String[] fs = x._1.split("##");
                Field f2k = new Field("qdlbm");
                f2k.setType(java.lang.String.class);
                Field f3k = new Field("hdlbm");
                f3k.setType(java.lang.String.class);
                Field f4k = new Field("zldwdm");
                f4k.setType(java.lang.String.class);
                r.put(fk, x._2);
                r.put(f2k, fs[0]);
                r.put(f3k, fs[1]);
                r.put(f4k, fs[2]);
                return new Tuple2<>(x._1, r);
            });

            StatLayer statLayerK = new StatLayer(statResultK);
            Field fk = new Field("mj");
            fk.setType(Double.class);
            statLayerK.getMetadata().addAttribute(fk, null);
            Field f2k = new Field("qdlbm");
            f2k.setType(String.class);
            statLayerK.getMetadata().addAttribute(f2k, null);
            Field f3k = new Field("hdlbm");
            f3k.setType(String.class);
            statLayerK.getMetadata().addAttribute(f3k, null);
            Field f4k = new Field("zldwdm");
            f4k.setType(String.class);
            statLayerK.getMetadata().addAttribute(f4k, null);


            LayerWriter statWriterK = LayerFactory.getWriter(ss, statsWriterConfig);
            statLayerK.inferFieldMetadata();
            statWriterK.write(statLayerK);

            PgLayerWriterConfig statsWriterConfigALL =  (PgLayerWriterConfig) statsWriterConfig;
            statsWriterConfigALL.setTablename(statsWriterConfigALL.getTablename()+"_all");
            LayerWriter statWriter = LayerFactory.getWriter(ss, statsWriterConfigALL);
            statLayer.inferFieldMetadata();
            statWriter.write(statLayer);





        } catch (Exception e) {
            e.printStackTrace();
            log.info(e.getLocalizedMessage());
            ifSuccess = false;
            log.error("Run error:" + e.getMessage());
            logBuilder.append("计算失败！" + e.getMessage() + "\r\n");
            pgHelper.runSQL(updateAnalysisLog(this.arg.getTaskName()), logBuilder.toString());
            throw new GISSparkException(e.getLocalizedMessage());
        } finally {
            pgHelper.runSQL(this.updateAnalysisInfo(this.arg.getTaskName())
                    , ifSuccess ? "SUCCESS" : "ERROR"
                    , java.sql.Timestamp.from(Instant.now()));

            logBuilder.append("计算结束..." + "\r\n");
            pgHelper.runSQL(this.updateAnalysisLog(this.arg.getTaskName()), logBuilder.toString());
            pgHelper.close();
        }
    }

    private MultiPolygonLayer read2dLayer(SparkSession ss, LayerReaderConfig config) throws Exception {
        LayerReader<MultiPolygonLayer> reader = LayerFactory.getReader(ss, config);

        MultiPolygonLayer pl = (MultiPolygonLayer) reader.read().repartitionToLayer(ss.sparkContext().defaultParallelism());
        JavaRDD<Tuple2<String, MultiPolygon>> tbFeatures = pl.map(new Function<Tuple2<String, MultiPolygon>, Tuple2<String, MultiPolygon>>() {
            @Override
            public Tuple2<String, MultiPolygon> call(Tuple2<String, MultiPolygon> in) throws Exception {

                Map<String, Object> attrs = in._2.getAttributesStr();
                MultiPolygon multiPolygon = new MultiPolygon(in._2);
                LinkedHashMap<Field, Object> resultAttrs = new LinkedHashMap<>();
                WKTReader reader = new WKTReader();

                if (String.valueOf(attrs.get("lx_id")).trim().length() > 0 && !String.valueOf(attrs.get("lx_id")).trim().equals("NO_DATA")) {
                    // 处理 lxdw feature
                    String lxWkt = String.valueOf(attrs.get("lx_wkt"));

                    org.locationtech.jts.geom.Point lx = (org.locationtech.jts.geom.Point) reader.read(lxWkt);
                    LinkedHashMap<Field, Object> attrsLx = new LinkedHashMap<>();
                    Field f1 = new Field("dlbm");
                    attrsLx.put(f1, attrs.get("lx_dlbm"));
                    Field f2 = new Field("mj");
                    f2.setType(java.lang.Double.class.getName());
                    attrsLx.put(f2, Double.valueOf(String.valueOf(attrs.get("lx_mj"))));
                    Field f3 = new Field("dlbz");
                    attrsLx.put(f3, attrs.get("lx_dlbz"));
                    Point lxf = new Point(String.valueOf(attrs.get("lx_id")), lx, attrsLx);
                    Field f0 = new Field("lxdw");
                    f0.setType(Point.class);
                    resultAttrs.put(f0, lxf);

                }

                if (String.valueOf(attrs.get("xz_id")).trim().length() > 0 && !String.valueOf(attrs.get("xz_id")).trim().equals("NO_DATA")) {
                    // 处理 xzdw feature
                    String xzWkt = String.valueOf(attrs.get("xz_wkt"));
                    MultiLineString xz = (MultiLineString) reader.read(xzWkt);
                    //LineString xz = (LineString) reader.read(xzWkt).getGeometryN(0);
                    LinkedHashMap<Field, Object> attrsXz = new LinkedHashMap<>();
                    Field f1 = new Field("dlbm");
                    attrsXz.put(f1, attrs.get("xz_dlbm"));

                    Field f2 = new Field("cd");
                    f2.setType(java.lang.Double.class);
                    attrsXz.put(f2, attrs.get("xz_cd"));

                    Field f3 = new Field("kd");
                    f3.setType(java.lang.Double.class);
                    attrsXz.put(f3, attrs.get("xz_kd"));

                    Field f4 = new Field("kcbl");
                    f3.setType(java.lang.Double.class);
                    attrsXz.put(f4, attrs.get("xz_kcbl"));
                    MultiPolyline xzf = new MultiPolyline(String.valueOf(attrs.get("xz_id")), xz, attrsXz);

                    Field f5 = new Field("dlbz");
                    attrsXz.put(f5, attrs.get("xz_dlbz"));

                    Field f0 = new Field("xzdw");
                    f0.setType(MultiPolyline.class);
                    resultAttrs.put(f0, xzf);
                }

                Field f1 = new Field("dlbm");
                resultAttrs.put(f1, attrs.get("DLBM"));

                Field f4 = new Field("bsm");
                resultAttrs.put(f4, attrs.get("BSM"));

                Field f2 = new Field("tbmj");
                f2.setType(java.lang.Double.class);
                resultAttrs.put(f2, attrs.get("TBMJ"));

                Field f3 = new Field("tkxs");
                f3.setType(java.lang.Double.class);
                resultAttrs.put(f3, attrs.get("TKXS"));//无zldwdm

                Field f5 = new Field("dlbz");
                resultAttrs.put(f5, attrs.get("DLBZ"));

                in._2.setAttributes(resultAttrs);
                multiPolygon.setAttributes(resultAttrs);
                return new Tuple2<>(in._1, multiPolygon);
            }
        });

        JavaRDD<Tuple2<String, MultiPolygon>> result = tbFeatures.map(new Function<Tuple2<String, MultiPolygon>, Tuple2<String, MultiPolygon>>() {
            @Override
            public Tuple2<String, MultiPolygon> call(Tuple2<String, MultiPolygon> ts) throws Exception {
                String key = ts._1;
                MultiPolygon feature = null;
                List<Tuple2<String, MultiPolygon>> in = new ArrayList<>();
                in.add(ts);
                Iterator<Tuple2<String, MultiPolygon>> tsi = in.iterator();
                while (tsi.hasNext()) {
                    Tuple2<String, MultiPolygon> t = tsi.next();
                    if (feature == null) {
                        String id = t._2.getFid();
                        org.locationtech.jts.geom.MultiPolygon g = t._2.getGeometry();
                        Map<String, Object> attrs = t._2.getAttributesStr();
                        LinkedHashMap<Field, Object> attrsO = new LinkedHashMap<>();
                        for (String k : attrs.keySet()) {
                            if (k.equals("xzdw")) {
                                MultiPolyline xz = (MultiPolyline) attrs.get(k);
                                Field f = new Field(k);
                                f.setType(MultiPolyline[].class);
                                attrsO.put(f, new MultiPolyline[]{xz});
                            } else if (k.equals("lxdw")) {
                                Point pf = (Point) attrs.get(k);
                                Field f = new Field(k);
                                f.setType(Point[].class);
                                attrsO.put(f, new Point[]{pf});
                            } else {
                                Field f = new Field(k);
                                attrsO.put(f, attrs.get(k));
                            }
                        }
                        feature = new MultiPolygon(id, g, attrsO);
                    } else {
                        // 进入这里的，都是至少有一个被关联 lxdw 或 xzdw 的tb
                        Map<String, Object> attrs = t._2.getAttributesStr();
                        for (String k : attrs.keySet()) {
                            if (k.equals("xzdw")) {
                                MultiPolyline xz = (MultiPolyline) attrs.get(k);
                                if (feature.getAttributesStr().keySet().contains(k)) {
                                    MultiPolyline[] opfs = (MultiPolyline[]) feature.getAttributesStr().get(k);
                                    for (MultiPolyline opf : opfs) {
                                        if (opf.getFid().equals(xz.getFid())) break;

                                        MultiPolyline[] rs = new MultiPolyline[opfs.length + 1];
                                        List<MultiPolyline> rl = new ArrayList<>(Arrays.asList(opfs));
                                        rl.add(xz);
                                        Field f = new Field(k);
                                        f.setType(MultiPolyline[].class);
                                        feature.getAttributes().put(f, rl.toArray(rs));
                                    }
                                } else {
                                    Field f = new Field(k);
                                    f.setType(MultiPolyline[].class);
                                    feature.getAttributes().put(f, new MultiPolyline[]{xz});
                                }
                            } else if (k.equals("lxdw")) {
                                Point lx = (Point) attrs.get(k);
                                if (feature.getAttributesStr().keySet().contains(k)) {
                                    Point[] opfs = (Point[]) feature.getAttributesStr().get(k);
                                    for (Point opf : opfs) {
                                        if (opf.getFid().equals(lx.getFid())) break;

                                        Point[] rs = new Point[opfs.length + 1];
                                        List<Point> rl = new ArrayList<>(Arrays.asList(opfs));
                                        rl.add(lx);
                                        Field f = new Field(k);
                                        f.setType(Point[].class);
                                        feature.getAttributes().put(f, rl.toArray(rs));
                                    }
                                } else {
                                    Field f = new Field(k);
                                    f.setType(Point[].class);
                                    feature.getAttributes().put(f, new Point[]{lx});
                                }
                            }
                        }
                    }
                }
                return new Tuple2<>(key, feature);
            }
        });

        return new MultiPolygonLayer(result.rdd());
    }

    //检查面自相交，防止有重复的点
    private MultiPolygon polygonFeatureEx(MultiPolygon polygonFeature) {
        Geometry geometryIn = polygonFeature.getGeometry();
        int geomNum = geometryIn.getNumGeometries();
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
        Polygon[] polygons = new Polygon[geomNum];
        for (int j = 0; j < geomNum; j++) {
            Polygon polygon = (Polygon) geometryIn.getGeometryN(j);
            LineString exter = ((org.locationtech.jts.geom.Polygon) polygon).getExteriorRing();
            Coordinate[] coordinates = exter.getCoordinates();
            List<Coordinate> coordinatesList = new ArrayList<>();

            List<Integer> cfint = new ArrayList<>();
            LinearRing ring;
            //面图斑最后一个点和第一个点相同
            for (int i = 0; i < coordinates.length - 1; i++) {
                if (coordinatesList.contains(coordinates[i])) {

                    //去除重复点
                    if (coordinates[i].getX() == coordinates[i - 1].getX() && coordinates[i].getY() == coordinates[i - 1].getY()) {
                        cfint.add(i);
                    } else {
                        //若后一点与该点重复
                        if (coordinates[i].getX() == coordinates[i + 1].getX() && coordinates[i].getY() == coordinates[i + 1].getY()) {
                            coordinates[i] = calNewCorrdinate(coordinates[i], coordinates[i + 2], coordinates[i - 1]);
                        } else {
                            coordinates[i] = calNewCorrdinate(coordinates[i], coordinates[i + 1], coordinates[i - 1]);
                        }
                    }
                }
                coordinatesList.add(coordinates[i]);
            }
            if (cfint.size() > 0) {
                Coordinate[] newCoordinates = new Coordinate[coordinates.length - cfint.size()];
                int k = 0;
                for (int i = 0; i < coordinates.length; i++) {

                    if (!cfint.contains(i)) {
                        newCoordinates[k] = coordinates[i];
                        k++;
                    }
                }
                ring = geometryFactory.createLinearRing(newCoordinates);
            } else {
                ring = geometryFactory.createLinearRing(coordinates);
            }


            //如果有内部环
            if (((org.locationtech.jts.geom.Polygon) polygon).getNumInteriorRing() > 0) {
                LinearRing[] holes = new LinearRing[((org.locationtech.jts.geom.Polygon) polygon).getNumInteriorRing()];

                for (int k = 0; k < ((org.locationtech.jts.geom.Polygon) polygon).getNumInteriorRing(); k++) {

                    LineString inRing = ((org.locationtech.jts.geom.Polygon) polygon).getInteriorRingN(k);
                    Coordinate[] inCoordinates = inRing.getCoordinates();
                    cfint = new ArrayList<>();
                    for (int i = 0; i < inCoordinates.length - 1; i++) {
                        if (coordinatesList.contains(inCoordinates[i])) {
                            //起始点另算
                            if (i == 0) {
                                inCoordinates[0] = calNewCorrdinate(inCoordinates[0], inCoordinates[1], inCoordinates[inCoordinates.length - 2]);
                                inCoordinates[inCoordinates.length - 1] = calNewCorrdinate(inCoordinates[0], inCoordinates[1], inCoordinates[inCoordinates.length - 2]);
                            } else {
                                //去除重复点
                                if (inCoordinates[i].getX() == inCoordinates[i - 1].getX() && inCoordinates[i].getY() == inCoordinates[i - 1].getY()) {
                                    cfint.add(i);
                                } else {
                                    //若后一点与该点重复
                                    if (coordinates[i].getX() == coordinates[i + 1].getX() && coordinates[i].getY() == coordinates[i + 1].getY()) {
                                        inCoordinates[i] = calNewCorrdinate(inCoordinates[i], inCoordinates[i + 2], inCoordinates[i - 1]);
                                    } else {
                                        inCoordinates[i] = calNewCorrdinate(inCoordinates[i], inCoordinates[i + 1], inCoordinates[i - 1]);
                                    }
                                }

                            }
                        }
                        coordinatesList.add(inCoordinates[i]);
                    }

                    if (cfint.size() > 0) {
                        Coordinate[] newCoordinates = new Coordinate[inCoordinates.length - cfint.size()];
                        int kk = 0;
                        for (int i = 0; i < inCoordinates.length; i++) {
                            if (!cfint.contains(i)) {
                                newCoordinates[kk] = inCoordinates[i];
                                kk++;
                            }
                        }
                        LinearRing inring = geometryFactory.createLinearRing(newCoordinates);
                        holes[k] = inring;

                    } else {
                        LinearRing inring = geometryFactory.createLinearRing(inCoordinates);
                        holes[k] = inring;
                    }
                }
                org.locationtech.jts.geom.Polygon geometryNew = geometryFactory.createPolygon(ring, holes);
                polygons[j] = geometryNew;

            } else {
                org.locationtech.jts.geom.Polygon geometryNew = geometryFactory.createPolygon(ring, null);
                polygons[j] = geometryNew;

            }

        }

        org.locationtech.jts.geom.MultiPolygon geometryNewall = geometryFactory.createMultiPolygon(polygons);
        polygonFeature.setGeometry(geometryNewall);
        return polygonFeature;
    }

    //返回点的修正点
    private Coordinate calNewCorrdinate(Coordinate coordinate, Coordinate coordinate1, Coordinate coordinate2) {
        double x1, x2, y1, y2;
        //不存在K的情况下
        if (coordinate1.getX() == coordinate.getX()) {
            x1 = coordinate.getX();
            if (coordinate1.getY() > coordinate.getY()) {
                y1 = coordinate.getY() + 0.01;
            } else {
                y1 = coordinate.getY() - 0.01;
            }
        } else {
            double k1 = (coordinate1.getY() - coordinate.getY()) / (coordinate1.getX() - coordinate.getX());
            //当K很大时，直线趋近与垂直X轴，这时X增加0.01，y值将会非常大
            if (Math.abs(k1) > 100) {
                x1 = coordinate.getX();
                if (coordinate1.getY() > coordinate.getY()) {
                    y1 = coordinate.getY() + 0.01;
                } else {
                    y1 = coordinate.getY() - 0.01;
                }
            } else {
                if (coordinate1.getX() > coordinate.getX()) {
                    y1 = coordinate.getY() + 0.01 * k1;
                    x1 = coordinate.getX() + 0.01;
                } else {
                    y1 = coordinate.getY() - 0.01 * k1;
                    x1 = coordinate.getX() - 0.01;
                }
            }
        }
        //不存在K的情况下
        if (coordinate2.getX() == coordinate.getX()) {
            x2 = coordinate.getX();
            if (coordinate2.getY() > coordinate.getY()) {
                y2 = coordinate.getY() + 0.01;
            } else {
                y2 = coordinate.getY() - 0.01;
            }
        } else {
            double k2 = (coordinate2.getY() - coordinate.getY()) / (coordinate2.getX() - coordinate.getX());
            //当K很大时，直线趋近与垂直X轴，这时X增加0.01，y值将会非常大
            if (Math.abs(k2) > 100) {
                x2 = coordinate.getX();
                if (coordinate2.getY() > coordinate.getY()) {
                    y2 = coordinate.getY() + 0.01;
                } else {
                    y2 = coordinate.getY() - 0.01;
                }
            } else {
                if (coordinate2.getX() > coordinate.getX()) {
                    y2 = coordinate.getY() + 0.01 * k2;
                    x2 = coordinate.getX() + 0.01;
                } else {
                    y2 = coordinate.getY() - 0.01 * k2;
                    x2 = coordinate.getX() - 0.01;
                }
            }
        }

        coordinate.setX((x1 + x2) / 2);
        coordinate.setY((y1 + y2) / 2);

        return coordinate;
    }

    //todo record state
    private String getIdByTaskName(String taskName) {
        return "SELECT id FROM " + this.pgConfig.getSchema() + ".mr_esdtask_record WHERE \"taskname\" = '" + taskName + "'";
    }

    private String insertAnalysisInfo() {
        return "INSERT INTO " + this.pgConfig.getSchema() + ".mr_esdtask_record (\"id\", \"taskname\", \"taskstate\"" +
                ", \"resultaddress\", \"params\", \"log\", \"submittime\") " +
                "VALUES (?,?,?,?,?,?,?)";
    }

    private String insertMJ() {
        return "INSERT INTO " + this.pgConfig.getSchema() + ".mr_xzq_area (\"id\", \"xzqdm\", \"xzqmc\"" +
                ", \"year\", \"mj\", \"mj_gq\", \"mj_mu\", \"createtime\") " +
                "VALUES (?,?,?,?,?,?,?,?)";
    }

    private String updateAnalysisInfo(String taskName) {
        return "UPDATE " + this.pgConfig.getSchema() + ".mr_esdtask_record SET " +
                "\"taskstate\" = ? ," +
                "\"finishtime\" = ? " +
                "WHERE \"taskname\" = '" + taskName + "'";
    }

    private String updateAnalysisLog(String taskName) {
        return "UPDATE " + this.pgConfig.getSchema() + ".mr_esdtask_record SET " +
                "\"log\" = ? " +
                "WHERE \"taskname\" = '" + taskName + "'";
    }

    @Override
    public void finish() {
//        PgHelper pgHelper = new PgHelper(this.pgConfig);
        log.info("Job Done.");
    }

    public static void main(String[] args) throws Exception {
        LandFlowAnalysis analysis = new LandFlowAnalysis(SparkSessionType.LOCAL, args);
        analysis.exec();
    }

}

