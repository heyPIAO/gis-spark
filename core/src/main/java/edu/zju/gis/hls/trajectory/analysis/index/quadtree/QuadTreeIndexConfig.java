package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

import edu.zju.gis.hls.trajectory.analysis.index.IndexConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString(callSuper = true)
public class QuadTreeIndexConfig extends IndexConfig {

    private boolean isClip;
    private int sampleSize;

    public QuadTreeIndexConfig() {
        this(false);
    }

    public QuadTreeIndexConfig(boolean isClip) {
        this(isClip, 1000);
    }

    public QuadTreeIndexConfig(int sampleSize) {
        this(false, sampleSize);
    }

    public QuadTreeIndexConfig(boolean isClip, int sampleSize) {
        this.isClip = isClip;
        this.sampleSize = sampleSize;
    }

    public QuadTreeIndexConfig(QuadTreeIndexConfig config) {
        this.isClip = config.isClip;
        this.sampleSize = config.sampleSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuadTreeIndexConfig that = (QuadTreeIndexConfig) o;
        return isClip == that.isClip;
    }


}
