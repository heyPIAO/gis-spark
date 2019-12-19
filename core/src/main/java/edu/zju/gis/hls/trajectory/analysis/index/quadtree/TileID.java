package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

import java.io.Serializable;

/**
 * Created by ylj on 2017/10/10.
 */
public class TileID implements Comparable,Serializable {
    private int zLevel;
    private int x;
    private int y;
    public TileID(){}

    /**
     * 对于单层运算, z都是一样的
     * @return
     */
    @Override
    public int hashCode() {
        // int result = getzLevel();
        // result = 31 * result + getX();
        // result = 31 * result + getY();
        return String.format("%d%d", x, y).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != TileID.class) return false;
        TileID objId = (TileID) obj;
        if (objId.x == this.x && objId.y == this.y && objId.zLevel == this.zLevel){
            return true;
        }else{
            return false;
        }
    }

    public TileID(int zLevel, int x, int y){
        this.zLevel = zLevel;
        this.x = x;
        this.y = y;
    }


    public int getzLevel() {
        return zLevel;
    }

    public void setzLevel(int zLevel) {
        this.zLevel = zLevel;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder().append(zLevel).append("_").append(x).append("_").append(y);
        return sb.toString();
    }

    public static TileID fromString(String str) {
        String[] zxy = str.split("_");
        TileID tileID = new TileID();
        try {
            tileID.setzLevel(Integer.valueOf(zxy[0]));
            tileID.setX(Integer.valueOf(zxy[1]));
            tileID.setY(Integer.valueOf(zxy[2]));
            return tileID;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public int compareTo(Object o) {
        TileID objId = (TileID) o;
        if (this.zLevel > objId.zLevel){
            return 1;
        }else if(this.zLevel < objId.zLevel){
            return -1;
        }else{
            if (this.x > objId.x){
                return 1;
            }else if(this.x < objId.x){
                return -1;
            }else{
                if (this.y > objId.y){
                    return 1;
                }else if(this.y < objId.y){
                    return -1;
                }else{
                    return 0;
                }
            }
        }
    }
}
