import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperFriends extends Mapper<LongWritable, Text, IntWritable, Text> {

    private IntWritable mapOutPutKey = new IntWritable();
    private Text mapOutPutValue = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String line = value.toString();
        String[] lineList = line.split(",");


        int id1 = Integer.parseInt(lineList[1]);
        int id2 = Integer.parseInt(lineList[2]);


        mapOutPutKey.set(id1);
        mapOutPutValue.set("Friend,1");
        context.write(mapOutPutKey, mapOutPutValue);

        mapOutPutKey.set(id2);
        mapOutPutValue.set("Friend,1");
        context.write(mapOutPutKey, mapOutPutValue);



    }

}