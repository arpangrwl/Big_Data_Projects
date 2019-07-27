import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperE extends Mapper<LongWritable, Text, IntWritable, Text> {
    private IntWritable mapOutPutKey = new IntWritable();
    private Text mapOutPutValue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String valueStr = value.toString();
        String[] valueList = valueStr.split(",");

        if(valueList.length > 0) {
            int userID = Integer.parseInt(valueList[1]);
            String pageID = valueList[2];

            mapOutPutKey.set(userID);
            mapOutPutValue.set(pageID);
            context.write(mapOutPutKey, mapOutPutValue);

        }


    }

}