import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public  class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
    String[] kCentroid;
    int k;
    int dimLen;
    @Override
    public void setup(Context context) {
        /*
            read current k centroid "x,y,z \n x,y,z"
         */
        Configuration conf = context.getConfiguration();
        k = Integer.parseInt(conf.get("k"));
        dimLen = Integer.parseInt(conf.get("dimLen"));
        kCentroid = conf.get("centroid").split("\n");
    }

    /*
        If first Iteration:
            value is point "x,y,z"
        Else:
            value is "x,y,z \t cluster"
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String samplePoint = value.toString();

        double minDis = Double.MAX_VALUE;
        String closestCentroid = kCentroid[0];
        for(String centroid: kCentroid){      //c1,c2 ["x,y,z" , "x,y,z"]
            double dis = euclidianDistance(centroid, samplePoint);
            if(dis < minDis){
                minDis = dis;
                closestCentroid = centroid;
            }
        }

        // key: closest centroid      value: one sampelPoint
        context.write(new Text(closestCentroid), new Text(samplePoint + ":1"));
    }





    /*
        Input "x1,y1,z1" "x1,y2,z2"
        Return Euclidian Distance between two points
     */
    public double euclidianDistance(String point1Str, String point2Str) {

        double [] point1 = new double[dimLen];
        double [] point2 = new double[dimLen];

        String[] point1List = point1Str.split(",");
        String[] point2List = point2Str.split(",");
        for(int i = 0; i < dimLen; i++){
            point1[i] = Float.parseFloat(point1List[i]);
            point2[i] = Float.parseFloat(point2List[i]);
        }


        double squaredSum = 0;
        for (int i = 0; i < dimLen; i++) {      //loop over dimension(2d)
            double diff = point1[i] - point2[i];
            squaredSum += diff*diff;
        }

        return Math.sqrt(squaredSum);
    }






}
