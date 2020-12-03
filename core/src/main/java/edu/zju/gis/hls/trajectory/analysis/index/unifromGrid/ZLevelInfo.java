package edu.zju.gis.hls.trajectory.analysis.index.unifromGrid;

import java.io.Serializable;

/**
 * Created by ylj on 2018/1/9.
 */
public class ZLevelInfo implements Serializable{
    public long getTotalCount() {
        return totalCount;
    }

    public int[] getTileRanges() {
        return tileRanges;
    }

    private long totalCount;
    private int[] tileRanges;

    public ZLevelInfo(long totalCount, int[] tileRanges) {
        this.totalCount = totalCount;
        this.tileRanges = tileRanges;
    }

    public ZLevelInfo(long totalCount, int tileX1, int tileX2, int tileY1, int tileY2) {
        this.totalCount = totalCount;
        this.tileRanges = new int[]{tileX1,tileX2,tileY1,tileY2};
    }

    public int getMinX() {
        return this.tileRanges[0];
    }

    public int getMaxX() {
        return this.tileRanges[1];
    }

    public int getMinY() {
        return this.tileRanges[2];
    }

    public int getMaxY() {
        return this.tileRanges[3];
    }

}
