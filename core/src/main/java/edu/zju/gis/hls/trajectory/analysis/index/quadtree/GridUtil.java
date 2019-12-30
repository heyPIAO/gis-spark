package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

import org.locationtech.jts.geom.*;

import static edu.zju.gis.hls.trajectory.analysis.model.Term.SCREEN_TILE_SIZE;

/**
 * Created by ylj on 2017/10/10.
 */
public class GridUtil {

    /**
     * 获取地图数据在基准地图中所覆盖的所有瓦片信息，且是每一层级的瓦片信息
     * @param pyramidConfig 瓦片金字塔配置信息
     * @param splitEnv 实际地图数据四至范围
     * @return
     */
    public static ZLevelInfo[] initZLevelInfoPZ(PyramidConfig pyramidConfig, Envelope splitEnv){
        int[] zLevelRange = pyramidConfig.getZLevelRange();
        Envelope baseMapEnv = pyramidConfig.getBaseMapEnv();
        ZLevelInfo[] result = new ZLevelInfo[zLevelRange[1] - zLevelRange[0] + 1];
        for (int i =  zLevelRange[0]; i <=  zLevelRange[1]; i++){
            int minX = (int) ((splitEnv.getMinX() - baseMapEnv.getMinX()) / pyramidConfig.getGridSize(i));
            int maxX = (int) ((splitEnv.getMaxX() - baseMapEnv.getMinX()) / pyramidConfig.getGridSize(i));
            int minY = (int) ((splitEnv.getMinY() - baseMapEnv.getMinY()) / pyramidConfig.getGridSize(i));
            int maxY = (int) ((splitEnv.getMaxY() - baseMapEnv.getMinY()) / pyramidConfig.getGridSize(i));
            long count =  (long)(maxX - minX + 1) * (maxY - minY + 1);
            result[i-zLevelRange[0]] = new ZLevelInfo(count, minX ,maxX, minY, maxY);
        }
        return result;
    }

    /**
     * 根据地图实际数据在金字塔中的层级信息反编码得到瓦片坐标
     * @param index             瓦片ID
     * @param zMin              金字塔中的最小层级
     * @param zLevelInfos       地图数据的层级信息
     * @return
     */
    public static GridID getTileID(long index, int zMin, ZLevelInfo[] zLevelInfos){
        GridID gridId = null;
        int leng = zLevelInfos.length;
        for (int z = 0; z < leng; z ++){
            if (index >= zLevelInfos[z].getTotalCount()){
                index -= zLevelInfos[z].getTotalCount();
            }else{
                gridId = new GridID();
                gridId.setzLevel(z + zMin);
                int width = (zLevelInfos[z].getTileRanges()[1]-zLevelInfos[z].getTileRanges()[0]);
                int x = (int)(index  % width);
                int y = (int)((index - x) / width);
                gridId.setX(x + zLevelInfos[z].getTileRanges()[0]);
                gridId.setY(y + zLevelInfos[z].getTileRanges()[2]);
                break;
            }
        }
        return gridId;
    }

    /**
     * 构建瓦片所对应的空间范围
     * @param gridID
     * @param pyramidConfig
     * @return
     */
    public static Envelope createTileBox(GridID gridID, PyramidConfig pyramidConfig){
        Envelope baseEnv = pyramidConfig.getBaseMapEnv();
        double minX = baseEnv.getMinX() + pyramidConfig.getGridSize(gridID.getzLevel()) * gridID.getX();
        double maxX = baseEnv.getMinX() + pyramidConfig.getGridSize(gridID.getzLevel()) * (gridID.getX() + 1);
        double minY = baseEnv.getMinY() + pyramidConfig.getGridSize(gridID.getzLevel()) * gridID.getY();
        double maxY = baseEnv.getMinY() + pyramidConfig.getGridSize(gridID.getzLevel()) * (gridID.getY() + 1);
        return new Envelope(minX, maxX, minY, maxY);
    }

    /**
     * 将外包矩形向外做缓冲区
     * @param env
     * @param gutter
     * @return
     */
    public static Envelope extendBy(Envelope env, double gutter){
        Envelope envelopeBuffered = new Envelope(env);
        envelopeBuffered.expandBy(gutter);
        return envelopeBuffered;
    }

    /**
     * 初始化瓦片金字塔中每一层级的瓦片表示的实际空间大小
     * @param zMin              金字塔最小层级
     * @param zMax              金字塔最大层级
     * @param baseMapEnv        金字塔基准底图
     * @return
     */
    public static double[] initGridSizes(int zMin, int zMax, Envelope baseMapEnv){
        double[] result = new double[zMax - zMin + 1];
        double mapSize = Math.min(baseMapEnv.getWidth(),baseMapEnv.getHeight());
        //double mapSize = Math.max(baseMapEnv.getWidth(),baseMapEnv.getHeight());
        int splitCount = 1 << zMin;
        for (int i = zMin; i <= zMax; i ++){
            result[i-zMin] = mapSize / splitCount;
            splitCount = splitCount << 1;
        }
        return result;
    }

    /**
     * 获取瓦片像素分辨率
     * @param zMax
     * @param baseMapEnv
     * @return
     */
    public static double[] initPixelSizesInTCrs(int zMax, Envelope baseMapEnv){
        double[] result = new double[zMax+1];
        double mapSize = Math.min(baseMapEnv.getWidth(),baseMapEnv.getHeight());
        int splitCount = 1;
        for (int i = 0; i <= zMax; i ++){
            result[i] = mapSize / (splitCount * SCREEN_TILE_SIZE);
            splitCount = splitCount << 1;
        }
        return result;
    }

    /**
     * 获取实际数据将要生成的全部瓦片数量
     * @param pyramidConfig
     * @param splitEnv
     * @return
     */
    public static long getTotalTileCount(PyramidConfig pyramidConfig, Envelope splitEnv){
        long sum = 0;
        int[] zLevelRange = pyramidConfig.getZLevelRange();
        Envelope baseEnv = pyramidConfig.getBaseMapEnv();
        for (int i = zLevelRange[0]; i <= zLevelRange[1]; i++){
            int minX = (int) ((splitEnv.getMinX() - baseEnv.getMinX()) / pyramidConfig.getGridSize(i));
            int maxX = (int) ((splitEnv.getMaxX() - baseEnv.getMinX()) / pyramidConfig.getGridSize(i));
            int minY = (int) ((splitEnv.getMinY() - baseEnv.getMinY()) / pyramidConfig.getGridSize(i));
            int maxY = (int) ((splitEnv.getMaxY() - baseEnv.getMinY()) / pyramidConfig.getGridSize(i));
            sum += (long)(maxX - minX + 1) * (maxY - minY + 1);
        }
        return sum;
    }

}
