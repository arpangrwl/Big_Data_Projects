import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperC1 extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String userStr = value.toString();
        String[] userList = userStr.split(",");

        if(userList.length > 0) {
            Text accessedPageID = new Text(userList[2]);
            context.write(accessedPageID, new IntWritable(1));
        }


    }

}