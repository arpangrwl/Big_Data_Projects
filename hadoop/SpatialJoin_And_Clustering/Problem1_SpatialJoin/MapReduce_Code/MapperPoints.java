import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperPoints extends Mapper<LongWritable,Text, IntWritable, Text> {
    Rectangular recWindow;

    public void setup(org.apache.hadoop.mapreduce.Mapper.Context context) {
        Configuration conf1 = context.getConfiguration();
        String window = conf1.get("windowLocation");
        recWindow = new Rectangular(window);
    }


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String pointStr = value.toString();
        Point p = new Point(pointStr);
        int pBlock = p.occupyBlock();

        if(recWindow.pointInRec(pointStr)){
            context.write(new IntWritable(pBlock), value);
        }

    }

}