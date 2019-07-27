import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperB extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String userStr = value.toString();
        String[] userList = userStr.split(",");

        if(userList.length > 0) {
            Text country = new Text(userList[2]);

            context.write(country, new IntWritable(1));
        }


    }

}