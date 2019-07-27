import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperG extends Mapper<LongWritable, Text, IntWritable, Text> {
    private IntWritable mapOutPutKey = new IntWritable();
    private Text mapOutPutValue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String valueStr = value.toString();
        String[] valueList = valueStr.split(",");

        //filter out dirty data
        if (valueList.length != 5){  //AccessLog and Friends all have 5 columns
            return;
        }

        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        //parse out Friends dataset
        if (fileName.contains("Friends")){
            int id = Integer.parseInt(valueList[1]);
            int friendID = Integer.parseInt(valueList[2]);
            mapOutPutKey.set(id);
            mapOutPutValue.set("Friends-"+friendID);
        }
        //parse out AccessLog dataset
        if (fileName.contains("AccessLog")){
            int id = Integer.parseInt(valueList[1]);
            int accessID = Integer.parseInt(valueList[2]);
            mapOutPutKey.set(id);
            mapOutPutValue.set("AccessLog-"+accessID);
        }

        context.write(mapOutPutKey,mapOutPutValue);

    }

}