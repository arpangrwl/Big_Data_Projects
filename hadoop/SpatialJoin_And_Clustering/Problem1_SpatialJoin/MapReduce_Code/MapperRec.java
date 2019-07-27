import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperRec extends Mapper<LongWritable,Text, IntWritable, Text> {

    Rectangular recWindow;

    public void setup(org.apache.hadoop.mapreduce.Mapper.Context context) {
        Configuration conf1 = context.getConfiguration();
        String window = conf1.get("windowLocation");
        recWindow = new Rectangular(window);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String rectangularStr = value.toString(); // region,location(4)
        Rectangular oneRectangular = new Rectangular(rectangularStr);

        if(recWindow.overlapWithAnotherRec(oneRectangular)){
            String recBlock = oneRectangular.occupyBlock();
            String windowBlock = recWindow.occupyBlock();


            for(String recBlockNum: recBlock.split(",")){
                for(String windowBlockNum: windowBlock.split(",")) {
                    int recBlockNumI = Integer.parseInt(recBlockNum);
                    int windowBlockNumI = Integer.parseInt(windowBlockNum);

                    if(recBlockNumI == windowBlockNumI){
                        context.write(new IntWritable(recBlockNumI), value);
                        break;
                    }

                }

            }
        }


    }

}