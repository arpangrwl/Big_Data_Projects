import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperF extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private IntWritable mapOutPutKey = new IntWritable();
    private IntWritable mapOutPutValue = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String valueStr = value.toString();
        String[] valueList = valueStr.split(",");

        if(valueList.length > 0) {
            int userID = Integer.parseInt(valueList[1]);  // people who access others
            int accessTime = Integer.parseInt(valueList[4]);  //access time

            mapOutPutKey.set(userID);
            mapOutPutValue.set(accessTime);
            context.write(mapOutPutKey, mapOutPutValue);

        }


    }

}