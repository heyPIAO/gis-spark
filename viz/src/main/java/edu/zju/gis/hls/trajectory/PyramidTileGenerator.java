package edu.zju.gis.hls.trajectory;

import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.PyramidConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.RasterImage;
import edu.zju.gis.hls.trajectory.analysis.operate.OperatorImpl;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.StatLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.file.FileImageWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.file.FileImageWriterConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.awt.*;
import java.awt.geom.GeneralPath;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static edu.zju.gis.hls.trajectory.analysis.model.Term.SCREEN_TILE_SIZE;

/**
 * 静态矢量瓦片生成器 (PNG)
 * @author Keran Sun (katus)
 * @version 1.0, 2020-08-13
 */
public class PyramidTileGenerator extends OperatorImpl {
  private final PyramidConfig config;
  private final FileImageWriter imageWriter;

  public PyramidTileGenerator(SparkSession ss, PyramidConfig config) {
    super(ss);
    this.config = config;
    imageWriter = new FileImageWriter(ss, new FileImageWriterConfig(config.getBaseDir()));
  }

  public JavaRDD<Tuple2<String, LinkedHashMap<Field, Object>>> generate(Layer layer) {
    // map 精简字段
    Layer<String, Feature> layer1 = layer.mapToLayer((Function<Tuple2<String, Feature>, Tuple2<String, Feature>>) pairItem -> {
      String[] keys = pairItem._1().split("_");
      Feature value = pairItem._2();
      LinkedHashMap<Field, Object> attr = new LinkedHashMap<>();
      // 新属性中增加瓦片编号
      attr.put(new Field("z"), keys[0]);
      attr.put(new Field("x"), keys[1]);
      attr.put(new Field("y"), keys[2]);
      // 新属性中保留颜色指定字段
      String colorValue = (String) value.getAttribute(config.getColorFieldName());
      attr.put(new Field(config.getColorFieldName()), colorValue);
      // 根据颜色字段设置颜色编码 置入属性
      String color, tmpColor = (String) config.getColorSymbol().get(colorValue);
      if (null == tmpColor || tmpColor.equals("")) {   // 如果没有对应小类则使用默认颜色
        color = config.getDefaultColor();
      } else {
        color = tmpColor;
      }
      attr.put(new Field("color"), color);
      // 修改属性表
      value.setAttributes(attr);
      return new Tuple2<>(pairItem._1(), value);
    });

    // 按照不同几何类型解析WKT
    Layer<String, Feature> layer2 = layer1.flatMapToLayer((FlatMapFunction<Tuple2<String, Feature>, Tuple2<String, Feature>>) pairItem -> {
      List<Tuple2<String, Feature>> result = new ArrayList<>();
      Feature feature = pairItem._2();
      String wkt = feature.getGeometryWkt();
      Feature nf;
      switch (feature.getGeometry().getGeometryType()) {
        case "Polygon": case "MultiPolygon":
          List<Feature> polygonList = new ArrayList<>();
          if (wkt.startsWith("POLYGON")) {
            wkt = wkt.replace("POLYGON ((", "").replace("))", "");
            nf = Feature.empty();
            nf.setFid(feature.getFid());
            nf.setAttributes(feature.getAttributes());
            nf.addAttribute(new Field("wkt"), wkt);
            polygonList.add(nf);
          } else if (wkt.startsWith("MULTIPOLYGON")) {
            wkt = wkt.replace("MULTIPOLYGON (((", "").replace(")))", "");
            String[] polygons = wkt.split("\\)\\), \\(\\(");
            for (String polygon : polygons) {
              nf = Feature.empty();
              nf.setFid(feature.getFid());
              nf.setAttributes(feature.getAttributes());
              nf.addAttribute(new Field("wkt"), polygon);
              polygonList.add(nf);
            }
          }
          // 处理多边形的内外环问题
          for (Feature polygon : polygonList) {
            String ringsWkt = (String) polygon.getAttribute("wkt");
            String[] rings = ringsWkt.split("\\), \\(");
            // 多边形外环必然存在
            nf = Feature.empty();
            nf.setFid(feature.getFid());
            nf.setAttributes(feature.getAttributes());
            nf.updateAttribute("wkt", rings[0]);
            result.add(new Tuple2<>(pairItem._1(), nf));
            // 多边形内环可能存在
            for (int i = 1; i < rings.length; i++) {
              nf = Feature.empty();
              nf.setFid(feature.getFid());
              nf.setAttributes(feature.getAttributes());
              nf.updateAttribute("color", config.getDefaultColor());
              nf.updateAttribute("wkt", rings[i]);
              result.add(new Tuple2<>(pairItem._1(), nf));
            }
          }
          break;
        case "LineString": case "MultiLineString":
          if (wkt.startsWith("LINESTRING")) {
            wkt = wkt.replace("LINESTRING (", "").replace(")", "");
            nf = Feature.empty();
            nf.setFid(feature.getFid());
            nf.setAttributes(feature.getAttributes());
            nf.addAttribute(new Field("wkt"), wkt);
            result.add(new Tuple2<>(pairItem._1(), nf));
          } else if (wkt.startsWith("MULTILINESTRING")) {
            wkt = wkt.replace("MULTILINESTRING ((", "").replace("))", "");
            String[] polylines = wkt.split("\\), \\(");
            for (String polyline : polylines) {
              nf = Feature.empty();
              nf.setFid(feature.getFid());
              nf.setAttributes(feature.getAttributes());
              nf.addAttribute(new Field("wkt"), polyline);
              result.add(new Tuple2<>(pairItem._1(), nf));
            }
          }
          break;
        case "Point": case "MultiPoint":
          if (wkt.startsWith("POINT")) {
            wkt = wkt.replace("POINT (", "").replace(")", "");
            nf = Feature.empty();
            nf.setFid(feature.getFid());
            nf.setAttributes(feature.getAttributes());
            nf.addAttribute(new Field("wkt"), wkt);
            result.add(new Tuple2<>(pairItem._1(), nf));
          } else if (wkt.startsWith("MULTIPOINT")) {
            wkt = wkt.replace("MULTIPOINT ", "").replace("(", "").replace(")", "");
            String[] points = wkt.split(", ");
            for (String point : points) {
              nf = Feature.empty();
              nf.setFid(feature.getFid());
              nf.setAttributes(feature.getAttributes());
              nf.addAttribute(new Field("wkt"), point);
              result.add(new Tuple2<>(pairItem._1(), nf));
            }
          }
          break;
        default:
          break;
      }
      return result.iterator();
    });

    // 地理坐标转化为像素坐标
    Layer<String, Feature> layer5 = layer2.mapToLayer((Function<Tuple2<String, Feature>, Tuple2<String, Feature>>) pairItem -> {
      StringBuilder strBuilder = new StringBuilder();
      Feature feature = pairItem._2();
      Integer col = Integer.parseInt((String) feature.getAttribute("x")), row = Integer.parseInt((String) feature.getAttribute("y"));
      Integer z = Integer.parseInt((String) feature.getAttribute("z"));
      Double a = config.getGridSize(z);
      String[] points = ((String) (feature.getAttribute("wkt"))).split(",");
      long tempX = -1, tempY = -1;
      for (String point : points) {
        String[] cor = point.trim().split(" ");
        long px = Math.round((Double.parseDouble(cor[0]) - config.getBaseMapEnv().getMinX() - col * a) / a * SCREEN_TILE_SIZE);
        long py = Math.round(SCREEN_TILE_SIZE - (Double.parseDouble(cor[1]) - config.getBaseMapEnv().getMinY() - row * a) / a * SCREEN_TILE_SIZE);
        if (!(px == tempX && py == tempY)) {
          strBuilder.append(px).append(" ").append(py).append(",");
        }
        tempX = px;
        tempY = py;
      }
      feature.updateAttribute("wkt", strBuilder.toString());
      return new Tuple2<>(pairItem._1(), feature);
    });

    // 按照键值进行合并 实在想不出使用封装方法的意义何在 所以直接拆封装了
    JavaRDD<Tuple2<String, LinkedHashMap<Field, Object>>> resultRdd = layer5.groupByKey()
      .map(item -> {
        BufferedImage img = new BufferedImage(SCREEN_TILE_SIZE, SCREEN_TILE_SIZE, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2d = img.createGraphics();
        for (Feature feature : item._2()) {
          Color c = Color.decode((String) feature.getAttribute("color"));
          g2d.setColor(c);
          String geoType = feature.getGeometry().getGeometryType();
          if (geoType.equals("Point") || geoType.equals("MultiPoint")) {
            String[] xy = ((String) (feature.getAttribute("wkt"))).replace(",", "").split(" ");
            g2d.fillOval(Integer.parseInt(xy[0]), Integer.parseInt(xy[1]), 5, 5);
          } else {
            GeneralPath gp = new GeneralPath(GeneralPath.WIND_EVEN_ODD);
            String[] points = ((String) (feature.getAttribute("wkt"))).split(",");
            if (points.length == 0) continue;
            String[] xy0 = points[0].split(" ");
            gp.moveTo(Double.parseDouble(xy0[0]), Double.parseDouble(xy0[1]));
            for (int i = 1; i < points.length; i++) {
              String[] xy = points[i].split(" ");
              gp.lineTo(Double.parseDouble(xy[0]), Double.parseDouble(xy[1]));
            }
            if (geoType.equals("Polygon")) {
              gp.closePath();
              g2d.fill(gp);
            } else {
              g2d.draw(gp);
            }
          }
        }
        g2d.dispose();
        LinkedHashMap<Field, Object> map = new LinkedHashMap<>();
        map.put(new Field("img"), new RasterImage(img));
        return new Tuple2<>(item._1(), map);
      });

    return resultRdd;
  }

  @Override
  public <T extends KeyIndexedLayer> Layer operate(T layer) {
    StatLayer resultLayer = new StatLayer(this.generate(layer.toLayer()));
    imageWriter.write(resultLayer);
    return resultLayer;
  }

  @Override
  public Layer operate(Layer layer) {
    // todo 也许存在更好的写法
    System.err.println("Need KeyIndexedLayer! Normal Layer is not ok.");
    return null;
  }
}
