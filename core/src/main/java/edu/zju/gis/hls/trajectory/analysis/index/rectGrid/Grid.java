package edu.zju.gis.hls.trajectory.analysis.index.rectGrid;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Created by ylj on 2017/10/10.
 * Updated by Hu
 */

public class Grid implements Comparable,Serializable {

    private int zLevel;

    @Getter
    @Setter
    private int x;

    @Getter
    @Setter
    private int y;

    public Grid(){}

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
        return String.format("%d_%d_%d", zLevel, x, y).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != Grid.class) return false;
        Grid objId = (Grid) obj;
        if (objId.x == this.x && objId.y == this.y && objId.zLevel == this.zLevel){
            return true;
        }else{
            return false;
        }
    }

    public Grid(int zLevel, int x, int y){
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder().append(zLevel).append("_").append(x).append("_").append(y);
        return sb.toString();
    }

    public static Grid fromString(String str) {
        String[] zxy = str.split("_");
        Grid grid = new Grid();
        try {
            grid.setzLevel(Integer.valueOf(zxy[0]));
            grid.setX(Integer.valueOf(zxy[1]));
            grid.setY(Integer.valueOf(zxy[2]));
            return grid;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public int compareTo(Object o) {
        Grid objId = (Grid) o;
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
