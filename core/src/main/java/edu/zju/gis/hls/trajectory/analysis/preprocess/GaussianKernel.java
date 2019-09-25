package edu.zju.gis.hls.trajectory.analysis.preprocess;

import org.apache.commons.math3.linear.MatrixUtils;

public class GaussianKernel extends Kernel {

    @Override
    public void create(int radius, float sigma) {

        if (radius < 1){
            throw new IllegalArgumentException("radius must be >= 1");
        }

        // 窗口宽度
        int width = 2*radius + 1;

        double[] data = new double[width];

        float twoSigmaSquare = 2.0f * sigma * sigma;

        float sigmaRoot = (float) Math.sqrt(twoSigmaSquare * Math.PI);

        float sum = 0.0f;

        for(int i=-radius; i<=radius; i++){
            float distance = i * i;
            int index = i + radius;
            data[index] = (float) Math.exp(-distance/twoSigmaSquare)/sigmaRoot;
            sum += data[index];
        }

        for(int i=0; i<=width-1; i++){
            data[i] = data[i] / sum;
        }

        this.m = MatrixUtils.createRowRealMatrix(data);

    }

    /**
     * 用于轨迹预处理的二维高斯分布函数
     * @param radius
     * @return
     */
    public void create2D(int radius, float sigma) {

        if (radius < 1){
            throw new IllegalArgumentException("radius must be >= 1");
        }

        float sum = 0.0f;

        // 窗口宽度
        int width = 2*radius + 1;


        float twoSigmaSquare = 2.0f * sigma * sigma;

        float sigmaRoot = (float) Math.PI * twoSigmaSquare;

        double[][] data = new double[width][width];


        float x, y;

        for(int i=-radius; i<=radius; i++){
            for(int j=-radius; j<=radius; j++){
                x = i*i;
                y = j*j;
                data[i][j] = (float)Math.exp(-(x+y)/twoSigmaSquare)/sigmaRoot;
                sum += data[i][j];
            }
        }

        for(int i=-radius; i<=radius; i++){
            for(int j=-radius; j<=radius; j++){
                data[i][j] = data[i][j]/sum;
            }
        }

        this.m = MatrixUtils.createRealMatrix(data);
    }


}
