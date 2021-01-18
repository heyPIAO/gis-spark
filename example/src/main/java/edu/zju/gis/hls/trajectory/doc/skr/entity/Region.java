package edu.zju.gis.hls.trajectory.doc.skr.entity;

import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.Serializable;

/**
 * @author Sun Katus
 * @version 1.0, 2021-01-15
 */
@Getter
@Setter
public class Region implements Serializable {
    private Integer level;
    private Integer index;
    private Envelope envelope;
    private Long startTime;
    private Long endTime;

    public Region(String line) throws ParseException {
        String[] items = line.split("\t");
        this.level = Integer.parseInt(items[0]);
        this.index = Integer.parseInt(items[1]);
        WKTReader reader = new WKTReader();
        String wkt = items[2].trim();
        this.envelope = reader.read(wkt).getEnvelopeInternal();
        this.startTime = Long.parseLong(items[3]);
        this.endTime = Long.parseLong(items[4]);
    }

    public double[] getMax(int d) {
        if (d > 3 || d < 2) return new double[0];
        double[] max = new double[d];
        max[0] = envelope.getMaxX();
        max[1] = envelope.getMaxY();
        if (d == 3) max[2] = endTime;
        return max;
    }

    public double[] getMin(int d) {
        if (d > 3 || d < 2) return new double[0];
        double[] min = new double[d];
        min[0] = envelope.getMinX();
        min[1] = envelope.getMinY();
        if (d == 3) min[2] = startTime;
        return min;
    }

    @Override
    public String toString() {
        return "Region{" +
                "level=" + level +
                ", index=" + index +
                ", envelope=[" + envelope.getMinX() + ", " + envelope.getMaxX() + ", " + envelope.getMinY() + ", " + envelope.getMaxY() +
                "], startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
