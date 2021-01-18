package edu.zju.gis.hls.trajectory.doc.skr.entity;

import com.github.davidmoten.rtreemulti.geometry.Point;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-15
 */
@Getter
@Setter
@ToString
public class CheckPoint implements Serializable {
    private Integer index;
    private Double x;
    private Double y;
    private Long t;

    public CheckPoint(String line) {
        String[] items = line.split("\t");
        this.index = Integer.parseInt(items[0]);
        this.x = Double.parseDouble(items[1]);
        this.y = Double.parseDouble(items[2]);
        this.t = Long.parseLong(items[3]);
    }

    public Point getKNNPoint() {
        return Point.create(x, y, t);
    }

    public double[] getValue() {
        return new double[]{x, y, t};
    }
}
