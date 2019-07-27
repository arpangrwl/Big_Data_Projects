import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperH2 extends Mapper<Text, Text, IntWritable, IntWritable> {


    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException{

        int count = Integer.parseInt(value.toString());
        context.write(new IntWritable(1), new IntWritable(count));
    }

}