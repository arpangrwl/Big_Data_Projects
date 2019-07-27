import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperA extends Mapper<LongWritable,Text,NullWritable,Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String userStr = value.toString();
        String[] userList = userStr.split(",");
        if(userList[2].equals("Chinese")){
            context.write(NullWritable.get(), value);
        }

    }

}