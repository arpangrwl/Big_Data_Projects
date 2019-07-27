import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ReducerH2 extends Reducer<IntWritable, IntWritable, Text, NullWritable> {
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{


        int count = 0;
        int totalSum = 0;

        for (IntWritable v: values){
            count += 1;
            totalSum += v.get();
        }
        double totalAvg  = (double)totalSum/count;

        context.write(new Text(""+totalAvg), NullWritable.get());

    }
}

