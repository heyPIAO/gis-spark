package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

import org.locationtech.jts.geom.Envelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import static edu.zju.gis.hls.trajectory.analysis.model.Term.SCREEN_TILE_SIZE;

/**
 * 瓦片金字塔配置信息
 * x与y方向分辨率相同
 * Created by ylj on 2018/1/9.
 * @version 2.0, 2020-08-13
 */
public class PyramidConfig implements Serializable {

  private Envelope extent = null;                         //金字塔表示的地理空间范围
  private int[] zLevelRange = null;                           //金字塔表示的显示层级范围
  private CoordinateReferenceSystem crs = null;               //金字塔表示的投影坐标系
  private double[] gridSizes;                                 //每一级瓦片表示的空间范围大小
  private double[] pixelResolution;                                 //每一级瓦片中的像素分辨率
  private String baseDir = "*";
  private String colorFieldName = "";
  private final Properties colorSymbol = new Properties();
  private final String defaultColor = "#FFFFFF";

  private PyramidConfig() {}

  public static class PyramidConfigBuilder {
    PyramidConfig config = new PyramidConfig();
    public PyramidConfigBuilder setBaseMapEnv(double minX, double maxX, double minY, double maxY){
      config.extent = new Envelope(minX, maxX, minY, maxY);
      return this;
    }

    public PyramidConfigBuilder setBaseMapEnv(Envelope e){
      config.extent = e;
      return this;
    }

    public PyramidConfigBuilder setZLevelRange(int minZLevel, int maxZLevel){
      config.zLevelRange = new int[]{minZLevel, maxZLevel};
      return this;
    }

    public PyramidConfigBuilder setCrs(CoordinateReferenceSystem crs){
      config.crs = crs;
      return this;
    }

    public PyramidConfigBuilder setColorFieldName(String colorFieldName) {
      config.colorFieldName = colorFieldName;
      return this;
    }

    public PyramidConfigBuilder setColorSymbol(String colorFilePath) throws IOException {
      config.colorSymbol.load(new FileInputStream(colorFilePath));
      return this;
    }

    public PyramidConfigBuilder setBaseDir(String dir) {
      config.baseDir = dir;
      return this;
    }

    public PyramidConfig build(boolean isLayerConfig) {
      if (checkConfigBuilder(isLayerConfig)) {
        config.gridSizes = GridUtil.initGridSizes(config.zLevelRange[0], config.zLevelRange[1], config.extent);
        config.pixelResolution = new double[config.gridSizes.length];
        int i = 0;
        for (double gridSise : config.gridSizes) {
          config.pixelResolution[i] = gridSise / SCREEN_TILE_SIZE;
          i++;
        }
        return config;
      }
      return null;
    }

    private boolean checkConfigBuilder(boolean isLayerConfig) {
      if (isLayerConfig) {
        return config.extent != null && config.crs != null && config.zLevelRange != null;
      } else {
        return config.extent != null && config.crs != null
          && config.zLevelRange != null && !config.colorFieldName.equals("")
          && !config.colorSymbol.isEmpty() && !config.baseDir.equals("*");
      }
    }
  }

  public Envelope getBaseMapEnv() {
    return this.extent;
  }

  public int[] getZLevelRange() {
    return this.zLevelRange;
  }

  public int getZMin() {
    return this.zLevelRange[0];
  }

  public int getZMax() {
    return this.zLevelRange[1];
  }

  public CoordinateReferenceSystem getCrs() {
    return this.crs;
  }

  public String getColorFieldName() {
    return colorFieldName;
  }

  public Properties getColorSymbol() {
    return colorSymbol;
  }

  public String getBaseDir() {
    return baseDir;
  }

  public String getDefaultColor() {
    return defaultColor;
  }

  public double getGridSize(int zLevel){
    return this.gridSizes[zLevel - zLevelRange[0]];
  }

}
