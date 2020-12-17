package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.rectGrid.RectGridIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.model.MultiPolygon;
import edu.zju.gis.hls.trajectory.analysis.model.Point;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.StatLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTReader;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
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

    private static Integer DEFAULT_INDEX_LEVEL = 10;

    public LandFlowAnalysis(SparkSessionType type, String[] args) {
        super(type, args);
    }

    @Override
    public void run() throws Exception {
        LayerReaderConfig tb3dReaderConfig = LayerFactory.getReaderConfig(this.arg.getTb3dReaderConfig());
        LayerReader<MultiPolygonLayer> tb3dLayerReader = LayerFactory.getReader(ss, tb3dReaderConfig);
        MultiPolygonLayer tb3dLayer = tb3dLayerReader.read();
        tb3dLayer.cache();
        LayerReaderConfig xz2dReaderConfig = LayerFactory.getReaderConfig(this.arg.getXz2dReaderConfig());
        MultiPolygonLayer xz2dLayer = this.read2dLayer(ss, xz2dReaderConfig);
        xz2dLayer.cache();

        DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.RECT_GRID, new RectGridIndexConfig(DEFAULT_INDEX_LEVEL, false));

        KeyIndexedLayer<MultiPolygonLayer> l1 = si.index(xz2dLayer, false);
        KeyIndexedLayer<MultiPolygonLayer> l2 = si.index(tb3dLayer, false);

        MultiPolygonLayer connected2d = l1.getLayer(); // key 为 TileID
        MultiPolygonLayer tb3dLayerPair = l2.getLayer(); // key 为 TileID

        JavaPairRDD<String, Tuple2<MultiPolygon, MultiPolygon>> temp = tb3dLayerPair.cogroup(connected2d).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<MultiPolygon>, Iterable<MultiPolygon>>>, String, Tuple2<MultiPolygon, MultiPolygon>>() {
            @Override
            public Iterator<Tuple2<String, Tuple2<MultiPolygon, MultiPolygon>>> call(Tuple2<String, Tuple2<Iterable<MultiPolygon>, Iterable<MultiPolygon>>> in) throws Exception {
                List<Tuple2<String, Tuple2<MultiPolygon, MultiPolygon>>> result = new ArrayList<>();
                for (MultiPolygon tb3d : in._2._1) {
                    for (MultiPolygon tb2d : in._2._2) {
                        if (tb3d.getGeometry().intersects(tb2d.getGeometry())) {
                            result.add(new Tuple2<>(tb3d.getFid() + "##" + tb2d.getFid(), new Tuple2<>(tb3d, tb2d)));
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
                tb3d = polygonFeatureEx(tb3d);
                tb2d = polygonFeatureEx(tb2d);

                try {
                    if (!tb3d.getGeometry().intersects(tb2d.getGeometry())) {
                        return result.iterator();
                    }
                    Point[] lx2d = (Point[]) t._2._2._2();
                    Polyline[] xz2d = (Polyline[]) t._2._2._3();

                    String fcc = String.valueOf(tb3d.getAttribute("DLBM"));
                    String zldwdm = String.valueOf(tb3d.getAttribute("ZLDWDM")).substring(0, 15);

                    //和 2调面状图斑 进行相交
                    String omzcc = String.valueOf(tb2d.getAttribute("dlbm"));
                    double tbArea = Double.valueOf(String.valueOf(tb2d.getAttribute("tbmj")));
                    double aarea = tb2d.getGeometry().getArea();

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

                            //线状计算
                            if (xz2d != null) {
                                for (Polyline lf : xz2d) {
                                    if (!g.intersects(lf.getGeometry())) continue;
                                    String oxzcc = String.valueOf(lf.getAttribute("dlbm"));
                                    try {
                                        Geometry ig = lf.getGeometry().intersection(g);
                                        double xzratio = ig.getLength() / lf.getGeometry().getLength();
                                        double oxzarea = xzratio * Double.valueOf(String.valueOf(lf.getAttribute("cd"))) * Double.valueOf(String.valueOf(lf.getAttribute("kd")));
                                        if (!oxzcc.equals(fcc)) {
                                            double okcbl = Double.valueOf(String.valueOf(lf.getAttribute("kcbl")));
                                            oxzarea = oxzarea * okcbl;
                                            dlbms.add(oxzcc);
                                            areas.add(oxzarea);

                                            flowTbArea = flowTbArea - oxzarea;
                                        }
                                    } catch (Exception e) {
                                        log.info("线状计算出错！");
                                        log.info(String.format("Intersection Error: 3D(%s) vs 2D(%s), abort", tb3d.toString(), lf.toString()));
                                    }
                                }
                            }

                            //点状地物进行计算
                            if (lx2d != null) {
                                for (Point pf : lx2d) {
                                    if (!g.contains(pf.getGeometry())) continue;
                                    String olxcc = String.valueOf(pf.getAttribute("dlbm"));
                                    double olxarea = (double) pf.getAttribute("mj");
                                    if (!olxcc.equals(fcc)) {
                                        dlbms.add(olxcc);
                                        areas.add(olxarea);
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
                                double kcarea = sdkcarea - edkcarea;

                                for (int j = 0; j < dlbms.size(); j++) {
                                    String occ = dlbms.get(j);
                                    double area = areas.get(j);
                                    if (flowTbArea - area < 0) {
                                        area = flowTbArea;
                                    }
                                    if (kcarea > 0) {

                                        //occ ## fcc ## ozldwdm ## fzldwdm ## area
                                        String kc = String.format("%s##1203##%s##%.9f", occ, zldwdm, area * sdkcxs);
                                        Tuple2<String, Geometry> flow = new Tuple2<>(kc, null);
                                        result.add(flow);

                                        String k = String.format("%s##%s##%s##%.9f", occ, fcc, zldwdm, area - area * sdkcxs);
                                        Tuple2<String, Geometry> flowk = new Tuple2<>(k, null);
                                        result.add(flowk);


                                        kcarea = kcarea - area * sdkcxs;
                                    } else {
                                        String k = String.format("%s##%s##%s##%.9f", occ, fcc, zldwdm, area);
                                        Tuple2<String, Geometry> flow = new Tuple2<>(k, null);
                                        result.add(flow);
                                    }
                                    flowTbArea = flowTbArea - area;
                                    if (flowTbArea < 0) {
                                        continue a;
                                    }
                                }

                                //面状地物情况
                                if (kcarea > 0) {
                                    String kc = String.format("%s##1203##%s##%.9f", omzcc, zldwdm, kcarea);
                                    Tuple2<String, Geometry> flow = new Tuple2<>(kc, null);
                                    result.add(flow);

                                    String k = String.format("%s##%s##%s##%.9f", omzcc, fcc, zldwdm, flowTbArea - kcarea);
                                    Tuple2<String, Geometry> flowk = new Tuple2<>(k, g);
                                    result.add(flowk);
                                } else {
                                    String k = String.format("%s##%s##%s##%.9f", omzcc, fcc, zldwdm, flowTbArea);
                                    Tuple2<String, Geometry> flow = new Tuple2<>(k, g);
                                    result.add(flow);
                                }


                            } else {
                                //相当于耕地到非耕地
                                flowTbArea = tbArea * iarea / aarea;
                                double kcarea = edkcarea - sdkcarea;

                                for (int j = 0; j < dlbms.size(); j++) {
                                    String occ = dlbms.get(j);
                                    double area = areas.get(j);
                                    if (flowTbArea - area < 0) {
                                        area = flowTbArea;
                                    }
                                    String k = String.format("%s##%s##%s##%.9f", occ, fcc, zldwdm, area);
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
                                String kc = String.format("123##%s##%s##%.9f", fcc, zldwdm, kcarea);
                                Tuple2<String, Geometry> flowkc = new Tuple2<>(kc, g);
                                result.add(flowkc);

                                //防止出现流转完田坎后面积用尽的特殊情况
                                flowTbArea = flowTbArea - kcarea;
                                if (flowTbArea < 0) {
                                    continue a;
                                }

                                String k = String.format("%s##%s##%s##%.9f", omzcc, fcc, zldwdm, flowTbArea);
                                Tuple2<String, Geometry> flow = new Tuple2<>(k, g);
                                result.add(flow);
                            }

                        } else {
                            //和线状图斑进行流转
                            if (xz2d != null) {
                                for (Polyline lf : xz2d) {
                                    if (!g.intersects(lf.getGeometry())) continue;
                                    String oxzcc = String.valueOf(lf.getAttribute("dlbm"));
                                    try {
                                        Geometry ig = lf.getGeometry().intersection(g);
                                        double xzratio = ig.getLength() / lf.getGeometry().getLength();
                                        double oxzarea = xzratio * Double.valueOf(String.valueOf(lf.getAttribute("cd"))) * Double.valueOf(String.valueOf(lf.getAttribute("kd")));
                                        if (!oxzcc.equals(fcc)) {
                                            double okcbl = Double.valueOf(String.valueOf(lf.getAttribute("kcbl")));
                                            oxzarea = oxzarea * okcbl;
                                            if (flowTbArea - oxzarea < 0) {
                                                oxzarea = flowTbArea;
                                            }
                                            if (sdkcarea > 0) {
                                                String kc = String.format("%s##1203##%s##%.9f", oxzcc, zldwdm, oxzarea * sdkcxs);
                                                Tuple2<String, Geometry> flow = new Tuple2<>(kc, ig);
                                                result.add(flow);

                                                String k = String.format("%s##%s##%s##%.9f", oxzcc, fcc, zldwdm, oxzarea - oxzarea * sdkcxs);
                                                Tuple2<String, Geometry> flowk = new Tuple2<>(k, ig);
                                                result.add(flowk);

                                                sdkcarea = sdkcarea - oxzarea * sdkcxs;
                                            } else {
                                                String k = String.format("%s##%s##%s##%.9f", oxzcc, fcc, zldwdm, oxzarea);
                                                Tuple2<String, Geometry> flow = new Tuple2<>(k, ig);
                                                result.add(flow);
                                            }

                                            flowTbArea = flowTbArea - oxzarea;
                                            if (flowTbArea < 0) {
                                                continue a;
                                            }
                                        }
                                    } catch (Exception e) {
                                        log.info("线状计算出错！");
                                        log.info(String.format("Intersection Error: 3D(%s) vs 2D(%s), abort", tb3d.toString(), lf.toString()));
                                    }
                                }
                            }

                            //和点状地物进行流转
                            if (lx2d != null) {
                                for (Point pf : lx2d) {
                                    if (!g.contains(pf.getGeometry())) continue;
                                    String olxcc = String.valueOf(pf.getAttribute("dlbm"));
                                    double olxarea = (double) pf.getAttribute("mj");
                                    if (flowTbArea - olxarea < 0) {
                                        olxarea = flowTbArea;
                                    }
                                    if (!olxcc.equals(fcc)) {
                                        if (sdkcarea > 0) {
                                            String kc = String.format("%s##1203##%s##%.9f", olxcc, zldwdm, olxarea * sdkcxs);
                                            Tuple2<String, Geometry> flow = new Tuple2<>(kc, pf.getGeometry());
                                            result.add(flow);

                                            String k = String.format("%s##%s##%s##%.9f", olxcc, fcc, zldwdm, olxarea - olxarea * sdkcxs);
                                            Tuple2<String, Geometry> flowk = new Tuple2<>(k, pf.getGeometry());
                                            result.add(flowk);

                                            sdkcarea = sdkcarea - olxarea * sdkcxs;
                                        } else {
                                            String k = String.format("%s##%s##%s##%.9f", olxcc, fcc, zldwdm, olxarea);
                                            Tuple2<String, Geometry> flow = new Tuple2<>(k, pf.getGeometry());
                                            result.add(flow);
                                        }
                                    }
                                    flowTbArea = flowTbArea - olxarea;
                                    if (flowTbArea < 0) {
                                        continue a;
                                    }
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
                                    String kc = String.format("123##%s##%s##%.9f", fcc, zldwdm, kcarea);
                                    Tuple2<String, Geometry> flowkc = new Tuple2<>(kc, g);
                                    result.add(flowkc);

                                    String k = String.format("%s##%s##%s##%.9f", omzcc, fcc, zldwdm, flowTbArea - kcarea);
                                    Tuple2<String, Geometry> flow = new Tuple2<>(k, g);
                                    result.add(flow);

                                } else {
                                    if (sdkcarea > 0) {
                                        String kc = String.format("%s##1203##%s##%.9f", omzcc, zldwdm, sdkcarea);
                                        Tuple2<String, Geometry> flow = new Tuple2<>(kc, g);
                                        result.add(flow);

                                        String k = String.format("%s##%s##%s##%.9f", omzcc, fcc, zldwdm, flowTbArea - sdkcarea);
                                        Tuple2<String, Geometry> flowk = new Tuple2<>(k, g);
                                        result.add(flowk);
                                    } else {
                                        String k = String.format("%s##%s##%s##%.9f", omzcc, fcc, zldwdm, flowTbArea);
                                        Tuple2<String, Geometry> flow = new Tuple2<>(k, g);
                                        result.add(flow);
                                    }
                                }

                            }
                        }
                    }
                } catch (Exception e) {
                    log.info("面状计算出错！");
                    log.info(String.format("Intersection Error: 3D(%s) vs 2D(%s), abort", tb3d.getAttribute("BSM"), tb2d.getFid()));
                }


                return result.iterator();
            }
        });

        intersected.cache();

        Layer intersectedLayer = new Layer(intersected.map(new Function<Tuple2<String, Geometry>, Tuple2<String, Feature>>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Geometry> in) throws Exception {
                return new Tuple2<>(in._1, new Feature(in._2));
            }
        }).rdd());

        LayerWriterConfig geomWriterConfig=LayerFactory.getWriterConfig(this.arg.getGeomWriterConfig());
        LayerWriter geomWriter = LayerFactory.getWriter(this.ss, geomWriterConfig);
        intersectedLayer.inferFieldMetadata();
        geomWriter.write(intersectedLayer);

        // 写出统计结果
        JavaPairRDD<String, Double> flowAreaRDD = intersected.mapToPair(new PairFunction<Tuple2<String, Geometry>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Geometry> in) throws Exception {
                String[] fs = in._1.split("##");
                String key = String.format("%s##%s##%s", fs[0], fs[1], fs[2]);
                Double v = Double.valueOf(fs[3]);
                return new Tuple2<>(key, v);
            }
        }).reduceByKey((x1, x2) -> x1 + x2);

        JavaRDD<Tuple2<String, LinkedHashMap<Field, Object>>> statResult = flowAreaRDD.map(x -> {
            LinkedHashMap<Field, Object> r = new LinkedHashMap<>();
            Field f = new Field("MJ");
            f.setType(java.lang.Double.class);
            Field f2 = new Field("ZLDWXX");
            r.put(f, x._2);
            r.put(f2, x._1);
            return new Tuple2<>(x._1, r);
        });

        StatLayer statLayer = new StatLayer(statResult);
        Field f = new Field("MJ");
        f.setType(Double.class);
        statLayer.getMetadata().addAttribute(f, null);
        Field f2 = new Field("ZLDWXX");
        statLayer.getMetadata().addAttribute(f2, null);

        LayerWriterConfig statsWriterConfig = LayerFactory.getWriterConfig(this.arg.getStatsWriterConfig());
        LayerWriter statWriter = LayerFactory.getWriter(ss, statsWriterConfig);
        statLayer.inferFieldMetadata();
        statWriter.write(statLayer);
    }

    private MultiPolygonLayer read2dLayer(SparkSession ss, LayerReaderConfig config) throws Exception {
        LayerReader<MultiPolygonLayer> reader = LayerFactory.getReader(ss, config);

        MultiPolygonLayer pl = reader.read();
        JavaRDD<Tuple2<String, MultiPolygon>> tbFeatures = pl.map(new Function<Tuple2<String, MultiPolygon>, Tuple2<String, MultiPolygon>>() {
            @Override
            public Tuple2<String, MultiPolygon> call(Tuple2<String, MultiPolygon> in) throws Exception {

                Map<String, Object> attrs = in._2.getAttributesStr();
                LinkedHashMap<Field, Object> resultAttrs = new LinkedHashMap<>();
                WKTReader reader = new WKTReader();

                if (String.valueOf(attrs.get("lx_id")).trim().length() > 0) {
                    // 处理 lxdw feature
                    String lxWkt = String.valueOf(attrs.get("lx_wkt"));
                    org.locationtech.jts.geom.Point lx = (org.locationtech.jts.geom.Point) reader.read(lxWkt);
                    LinkedHashMap<Field, Object> attrsLx = new LinkedHashMap<>();
                    Field f1 = new Field("dlbm");
                    attrsLx.put(f1, attrs.get("lx_dlbm"));
                    Field f2 = new Field("mj");
                    f2.setType(java.lang.Double.class.getName());
                    attrsLx.put(f2, Double.valueOf(String.valueOf(attrs.get("lx_mj"))));
                    Point lxf = new Point(String.valueOf(attrs.get("lx_id")), lx, attrsLx);

                    Field f0 = new Field("lxdw");
                    f0.setType(Point.class);
                    resultAttrs.put(f0, lxf);
                }

                if (String.valueOf(attrs.get("xz_id")).trim().length() > 0) {
                    // 处理 xzdw feature
                    String xzWkt = String.valueOf(attrs.get("xz_wkt"));
                    LineString xz = (LineString) reader.read(xzWkt);
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
                    Polyline xzf = new Polyline(String.valueOf(attrs.get("xz_id")), xz, attrsXz);

                    Field f0 = new Field("xzdw");
                    f0.setType(Polyline.class);
                    resultAttrs.put(f0, xzf);
                }

                Field f1 = new Field("dlbm");
                resultAttrs.put(f1, attrs.get("tb_dlbm"));

                Field f2 = new Field("tbmj");
                f2.setType(java.lang.Double.class);
                resultAttrs.put(f2, attrs.get("tb_tbmj"));

                Field f3 = new Field("tkxs");
                f3.setType(java.lang.Double.class);
                resultAttrs.put(f3, attrs.get("tb_tkxs"));

                in._2.setAttributes(resultAttrs);
                return in;
            }
        });

        JavaRDD<Tuple2<String, MultiPolygon>> result = tbFeatures.groupBy(x -> x._1).map(new Function<Tuple2<String, Iterable<Tuple2<String, MultiPolygon>>>, Tuple2<String, MultiPolygon>>() {
            @Override
            public Tuple2<String, MultiPolygon> call(Tuple2<String, Iterable<Tuple2<String, MultiPolygon>>> ts) throws Exception {
                String key = ts._1;
                MultiPolygon feature = null;
                Iterator<Tuple2<String, MultiPolygon>> tsi = ts._2.iterator();
                while (tsi.hasNext()) {
                    Tuple2<String, MultiPolygon> t = tsi.next();
                    if (feature == null) {
                        String id = t._2.getFid();
                        org.locationtech.jts.geom.MultiPolygon g = t._2.getGeometry();
                        Map<String, Object> attrs = t._2.getAttributesStr();
                        LinkedHashMap<Field, Object> attrsO = new LinkedHashMap<>();
                        for (String k : attrs.keySet()) {
                            if (k.equals("xzdw")) {
                                Polyline xz = (Polyline) attrs.get(k);
                                Field f = new Field(k);
                                f.setType(Polyline[].class);
                                attrsO.put(f, new Polyline[]{xz});
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
                                Polyline xz = (Polyline) attrs.get(k);
                                if (feature.getAttributesStr().keySet().contains(k)) {
                                    Polyline[] opfs = (Polyline[]) feature.getAttributesStr().get(k);
                                    for (Polyline opf : opfs) {
                                        if (opf.getFid().equals(xz.getFid())) break;
                                    }
                                    Polyline[] rs = new Polyline[opfs.length + 1];
                                    List<Polyline> rl = new ArrayList<>(Arrays.asList(opfs));
                                    rl.add(xz);
                                    Field f = new Field(k);
                                    f.setType(Polyline[].class);
                                    feature.getAttributes().put(f, rl.toArray(rs));
                                } else {
                                    Field f = new Field(k);
                                    f.setType(Polyline[].class);
                                    feature.getAttributes().put(f, new Polyline[]{xz});
                                }
                            } else if (k.equals("lxdw")) {
                                Point lx = (Point) attrs.get(k);
                                if (feature.getAttributesStr().keySet().contains(k)) {
                                    Point[] opfs = (Point[]) feature.getAttributesStr().get(k);
                                    for (Point opf : opfs) {
                                        if (opf.getFid().equals(lx.getFid())) break;
                                    }
                                    Point[] rs = new Point[opfs.length + 1];
                                    List<Point> rl = new ArrayList<>(Arrays.asList(opfs));
                                    rl.add(lx);
                                    Field f = new Field(k);
                                    f.setType(Point[].class);
                                    feature.getAttributes().put(f, rl.toArray(rs));
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


    public static void main(String[] args) throws Exception {
        LandFlowAnalysis analysis = new LandFlowAnalysis(SparkSessionType.LOCAL, args);
        analysis.exec();
    }

}
