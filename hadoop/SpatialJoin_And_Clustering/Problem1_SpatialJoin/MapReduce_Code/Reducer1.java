import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import java.util.*;

public class Reducer1 extends Reducer<IntWritable, Text, IntWritable, Text> {


    Rectangular recWindow;

    public void setup(Context context) {
        Configuration conf1 = context.getConfiguration();
        String window = conf1.get("windowLocation");
        recWindow = new Rectangular(window);
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        Set<String> recSet = new LinkedHashSet<>(); //store rec
        Set<String> pointsSet = new LinkedHashSet<>(); //store points

        //store points and rectangular in different set
        for(Text v: values){
            String[] vList = v.toString().split(",");
            if(vList.length == 2){  // points in this block
                pointsSet.add(v.toString());
            }else if(vList.length == 5) { // rectangular in this block
                recSet.add(v.toString());
            }
        }


        for(String p: pointsSet){
            Point point = new Point(p);//point is unique

            for(String r: recSet){
                Rectangular rec = new Rectangular(r);

                if(rec.pointInRec(p)){
                    context.write(new IntWritable(rec.region),new Text(point.toString()));
                }

            }


        }


    }
}

