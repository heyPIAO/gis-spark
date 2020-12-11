package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QuadTreeIndexLayer<L extends Layer> extends KeyIndexedLayer<L> {
    public QuadTreeIndexLayer(QuadTreePartitioner partitioner)
    {
        super(partitioner);
        this.indexType = IndexType.QUAD_TREE;
    }
}
