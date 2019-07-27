import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperC2 extends Mapper<Text, Text, IntWritable, Text> {
    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException{

        //input: key'11 id'     value(cnt):'1'
        IntWritable count = new IntWritable(Integer.parseInt(value.toString()));

        context.write(count, key); // reverse key/value  (sort by count)


    }

}