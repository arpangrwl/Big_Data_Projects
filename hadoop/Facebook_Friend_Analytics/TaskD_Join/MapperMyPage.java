import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MapperMyPage extends Mapper<LongWritable, Text, IntWritable, Text> {

    private IntWritable mapOutPutKey = new IntWritable();
    private Text mapOutPutValue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String[] lineList = line.split(",");

        int id = Integer.parseInt(lineList[0]);
        String name = lineList[1];

        mapOutPutKey.set(id);
        mapOutPutValue.set("Name,"+ name);


        context.write(mapOutPutKey, mapOutPutValue);


    }

}