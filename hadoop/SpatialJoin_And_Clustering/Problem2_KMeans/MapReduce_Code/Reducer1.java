import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

public class Reducer1 extends Reducer<Text,Text, Text,NullWritable> {

    Counter counter = null;
    int k;
    int dimLen;
    int curIteration;
    double oldNewCentroidDistanceSum=0;

    @Override
    public void setup(Reducer.Context context) {
        Configuration conf = context.getConfiguration();
        k = Integer.parseInt(conf.get("k"));
        dimLen = Integer.parseInt(conf.get("dimLen"));
        curIteration = Integer.parseInt(conf.get("iteration"));
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        LinkedList<String> valueList = new LinkedList<>();

        // sum  stores all point sum in each dimension belongs to this centroid
        // count stores number of points belongs to this centroid
        double[] sum = new double[k];
        int count = 0;

        for(Text v: values) {
            String value = v.toString();
            valueList.addLast(value);
            count += Integer.parseInt(value.split(":")[1]);
            String[] pointLoc = value.split(":")[0].split(",");
            for(int i = 0; i < dimLen; i++) {
                sum[i] += Double.parseDouble(pointLoc[i]);
            }
        }

        // compute new centroid
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < dimLen; i++) {
            sb.append(sum[i]/= count);
            if(i!=(dimLen-1)){
                sb.append(",");
            }
        }
        String newCentroid = sb.toString().trim(); //new "x_c,y_c,z_c"
        String oldCentroid = key.toString();
        oldNewCentroidDistanceSum += euclidianDistance(oldCentroid, newCentroid);

        context.write(new Text(newCentroid), NullWritable.get());
    }

    // write out the new centroid
    public void cleanup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);

        Path outputPath1 = new Path("/user/ds503/project2/problem3/output/iter" + (curIteration)+"/oldNewCentroidDistanceSum.csv");
        FSDataOutputStream FSDataOutputStream1 = hdfs.create(outputPath1);
        FSDataOutputStream1.writeBytes(""+oldNewCentroidDistanceSum);
        FSDataOutputStream1.close();
    }



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
