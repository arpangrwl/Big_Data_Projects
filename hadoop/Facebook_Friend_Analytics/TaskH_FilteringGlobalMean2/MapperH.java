import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperH extends Mapper<LongWritable, Text, IntWritable, IntWritable> {


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String line = value.toString();
        String[] lineList = line.split(",");

        if(lineList.length > 0) {
            int id1 = Integer.parseInt(lineList[1]);
            int id2 = Integer.parseInt(lineList[2]);
            context.write(new IntWritable(id1), new IntWritable(1));
            context.write(new IntWritable(id2), new IntWritable(1));
        }


    }

}