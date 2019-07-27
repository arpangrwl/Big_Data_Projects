import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class ReducerF extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private final int timeThreshold = 900000;
    //private Text reduceOutPutValue = new Text();
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        int lastTime = 0;
        for(IntWritable v : values) {
            int time = v.get();
            if(time > lastTime){
                lastTime = time;
            }
        }

        if( timeThreshold > lastTime){
            context.write(key,new IntWritable(lastTime));
        }



    }
}
