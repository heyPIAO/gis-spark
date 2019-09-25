package edu.zju.gis.hls.trajectory.analysis.preprocess;

import lombok.Getter;
import org.apache.commons.math3.linear.RealMatrix;

@Getter
public abstract class Kernel implements Cloneable {

    protected RealMatrix m;

    /**
     * 构建二维过滤模板
     * 默认 sigma
     * @param radius
     */
    public void create2D(int radius){
        this.create(radius, radius/3.0f);
    }

    public abstract void create2D(int radius, float sigma);

    /**
     * 构建一维过滤模板
     * 默认 sigma
     * @param radius
     */
    public void create(int radius){
        this.create(radius, radius/3.0f);
    }

    public abstract void create(int radius, float sigma);

}
