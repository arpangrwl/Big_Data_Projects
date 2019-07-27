import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class Mapper1 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String Evaluation = value.toString().split(",")[8];
        int evaluation = Integer.parseInt(Evaluation.split(": ")[1]);

        context.write(new IntWritable(evaluation), new IntWritable(1));


    }






}

