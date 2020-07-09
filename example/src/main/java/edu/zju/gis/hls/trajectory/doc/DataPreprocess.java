package edu.zju.gis.hls.trajectory.doc;


import edu.zju.gis.hls.trajectory.analysis.model.TrajectoryPoint;
import edu.zju.gis.hls.trajectory.analysis.preprocess.GaussianKernel;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class DataPreprocess {

    public static void main(String[] args) throws IOException {
        String pStr ="D:\\Work\\Study\\GeRenXueXi\\笔记\\大规模轨迹数据计算与服务关键技术研究\\data\\rider\\20170920.txt";
        String oStr = "D:\\Work\\Study\\GeRenXueXi\\笔记\\大规模轨迹数据计算与服务关键技术研究\\data\\rider\\20170920_wkt.txt";
        File out = new File(oStr);
        File in = new File(pStr);
        if (!in.exists()) {
            throw new FileNotFoundException("cannot find file " + pStr);
        }
        BufferedReader reader = Files.newBufferedReader(in.toPath());
        BufferedWriter writer = new BufferedWriter(new FileWriter(out));
        List<String> result = new ArrayList<>();
        String line = reader.readLine();
        int total = 0;
        while (line != null) {
            String[] fields = line.split("\t");
            float y = Float.valueOf(fields[4]);
            float x = Float.valueOf(fields[5]);
            Coordinate coordinate = new Coordinate(x, y);
            Point point = new GeometryFactory().createPoint(coordinate);
            String wkt = point.toString();
            List<String> resultFeature = new ArrayList<>();
            resultFeature.add(fields[0]);
            resultFeature.add(fields[1]);
            resultFeature.add(fields[2]);
            resultFeature.add(fields[3]);
            resultFeature.add(fields[6]);
            resultFeature.add(wkt);
            String r = String.join("\t", resultFeature);
            result.add(r);
            writer.write(r + "\r\n");
            if (result.size() == 100) {
                writer.flush();
                total += 100;
                result.clear();
                System.out.println("count " + total);
            }
            line = reader.readLine();
        }
        writer.flush();
        total += result.size();
        System.out.println("Total count " + total);
    }

    /**
     * 数据过滤
     * @param points
     * @return
     */
    public static List<TrajectoryPoint> filter(List<TrajectoryPoint> points, String fieldname, Object fieldValue) {

        List<TrajectoryPoint> temp = new ArrayList<>();

        for(int i=0; i<points.size(); i++) {
            if(points.get(i).getAttribute(fieldname).equals(fieldValue))
                temp.add(points.get(i));
        }
        return temp;
    }


    /**
     * 语义矫正
     * 对 leaving 过程中的数据进行语义矫正
     * leaving 语义为骑手在店内取餐，因此应只有一个点
     * 空间位置以 leave点为准，时间以 leaving 的第1个点的时间为准
     * @param points
     */
    public static void semanticAdjustment(List<TrajectoryPoint> points) {

        int m = 0;
        boolean flagL = false;
        TrajectoryPoint temp = null;
        for (m=0; m<points.size(); m++) {
            TrajectoryPoint p = points.get(m);
            if (String.valueOf(p.getAttribute("status")).equals("leaving")){
                // 如果是第一个leaving点
                if (!flagL) {
                   flagL = true;
                   if (temp == null){
                       temp = p;
                   } else {
                       temp.setTimestamp(p.getTimestamp());
                   }
                }
                points.remove(m);
            } else if (String.valueOf(p.getAttribute("status")).equals("leave")) {
                if (temp == null) {
                    temp = p;
                } else {
                    temp.setGeometry(p.getGeometry());
                }
            }
        }
        points.add(temp);
    }


    /**
     * 数据去重
     * @param points 按时间顺序排列
     */
    public static void replication(List<TrajectoryPoint> points){

    }

    /**
     * 高斯平滑
     * 以 GPS 点位前后 2 个点的数据进行加权平滑
     * 仅对 arriving 和 delivering 进行高斯过滤
     * @param points 按时间戳排序的轨迹点
     */
    public static List<TrajectoryPoint> gaussFilter(List<TrajectoryPoint> points) {

        List<TrajectoryPoint> result = new ArrayList<>();

        GaussianKernel gaussianKernel = new GaussianKernel();

        int w = 5;
        int radius = 2;
        int mid = w-radius-1; // start from 0

        gaussianKernel.create(radius);

        for (int i=0; i<points.size(); i++) {
            double[] dataX = new double[w];
            double[] dataY = new double[w];
            TrajectoryPoint p = points.get(i);
            if (!(String.valueOf(p.getAttribute("status")).equals("arriving")
                    || String.valueOf(p.getAttribute("status")).equals("deliverying"))){
                result.add(p);
                continue;
            }

            dataX[mid] = p.getGeometry().getX();
            dataY[mid] = p.getGeometry().getY();

            // 获取 window 内的值
            // mid left
            for (int m=mid-1; m>=0; m--){
                int index = i-(mid-m);
                if(index < 0){
                    dataX[m] = dataX[m+1];
                    dataY[m] = dataY[m+1];
                } else {
                    dataX[m] = points.get(m).getGeometry().getX();
                    dataY[m] = points.get(m).getGeometry().getY();
                }
            }

            // mid right
            for (int m=mid+1; m<=w-1; m++){
                int index = i-(m-mid);
                if(index >= points.size()){
                    dataX[m] = dataX[m-1];
                    dataY[m] = dataY[m-1];
                } else {
                    dataX[m] = points.get(m).getGeometry().getX();
                    dataY[m] = points.get(m).getGeometry().getY();
                }
            }

            // 将高斯过滤核作用于 window
            RealMatrix m = gaussianKernel.getM();
            RealMatrix xM = MatrixUtils.createRowRealMatrix(dataX);
            RealMatrix yM = MatrixUtils.createRowRealMatrix(dataY);

            RealMatrix resultX = xM.multiply(m.transpose());
            RealMatrix resultY = yM.multiply(m.transpose());

            Coordinate c = new Coordinate(resultX.getEntry(0, 0), resultY.getEntry(0, 0));
            Geometry point = new GeometryFactory().createPoint(c);

            TrajectoryPoint resultP = p;
            resultP.setGeometry((Point)point);

            result.add(resultP);
        }
        return result;

    }



}
