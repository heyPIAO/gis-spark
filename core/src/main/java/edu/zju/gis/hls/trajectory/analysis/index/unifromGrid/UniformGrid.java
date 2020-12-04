package edu.zju.gis.hls.trajectory.analysis.index.unifromGrid;

import java.io.Serializable;

/**
 * Created by ylj on 2017/10/10.
 * Updated by Hu
 */
public class UniformGrid implements Comparable,Serializable {

    private int zLevel;
    private int x;
    private int y;

    public UniformGrid(){}

    /**
     * TODO A better hashcode for having a more balanced distribution
     * hint: z,x,y has it's own limitation, maybe it's a way
     * @return
     */
    @Override
    public int hashCode() {
        // int result = getzLevel();
        // result = 31 * result + getX();
        // result = 31 * result + getY();
        return String.format("%d%d%d", zLevel, x, y).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != UniformGrid.class) return false;
        UniformGrid objId = (UniformGrid) obj;
        if (objId.x == this.x && objId.y == this.y && objId.zLevel == this.zLevel){
            return true;
        }else{
            return false;
        }
    }

    public UniformGrid(int zLevel, int x, int y){
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

    public static UniformGrid fromString(String str) {
        String[] zxy = str.split("_");
        UniformGrid uniformGrid = new UniformGrid();
        try {
            uniformGrid.setzLevel(Integer.valueOf(zxy[0]));
            uniformGrid.setX(Integer.valueOf(zxy[1]));
            uniformGrid.setY(Integer.valueOf(zxy[2]));
            return uniformGrid;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public int compareTo(Object o) {
        UniformGrid objId = (UniformGrid) o;
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
