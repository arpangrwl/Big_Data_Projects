import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;

public class Combiner1 extends Reducer<Text,Text, Text,Text> {

    int dimLen;
    public void setup(Reducer.Context context) {
        Configuration conf = context.getConfiguration();
        dimLen = Integer.parseInt(conf.get("dimLen"));

    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        double[] sum = new double[dimLen];

        for(Text value: values){
            String v = value.toString();
            count += Integer.parseInt(v.split(":")[1]);
            String[] pointLoc = v.split(":")[0].split(",");
            for(int i = 0; i < dimLen; i++){
                sum[i] += Double.parseDouble(pointLoc[i]);
            }
        }


        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < dimLen; i++) {
            sb.append(sum[i]);
            if(i!=(dimLen-1)){
                sb.append(",");
            }
        }
        sb.append(":" + count);

        context.write(key, new Text(sb.toString()));

    }




}
