package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

import org.locationtech.jts.geom.Envelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;

/**
 * 瓦片金字塔配置信息
 * x与y方向分辨率相同
 * Created by ylj on 2018/1/9.
 */
public class PyramidConfig implements Serializable{

    private Envelope extent = null;                         //金字塔表示的地理空间范围
    private int[] zLevelRange = null;                           //金字塔表示的显示层级范围
    private CoordinateReferenceSystem crs = null;               //金字塔表示的投影坐标系

    private PyramidConfig(){
    }

    public static class PyramidConfigBuilder{
        PyramidConfig config = new PyramidConfig();
        public PyramidConfigBuilder setBaseMapEnv(double minX, double maxX, double minY, double maxY){
            config.extent = new Envelope(minX, maxX, minY, maxY);
            return this;
        }

        public PyramidConfigBuilder setzLevelRange(int minZLevel, int maxZLevel){
            config.zLevelRange = new int[]{minZLevel, maxZLevel};
            return this;
        }

        public PyramidConfigBuilder setCrs(CoordinateReferenceSystem crs){
            config.crs = crs;
            return this;
        }

        public PyramidConfig build(){
            if (config.extent != null && config.crs != null && config.zLevelRange != null){
                return config;
            }
            return null;
        }
    }

    public Envelope getBaseMapEnv() {
        return this.extent;
    }

    public int[] getZLevelRange() {
        return this.zLevelRange;
    }

    public CoordinateReferenceSystem getCrs() {
        return this.crs;
    }

}
