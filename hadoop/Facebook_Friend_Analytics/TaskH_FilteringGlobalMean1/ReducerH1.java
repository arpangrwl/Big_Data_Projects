import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ReducerH1 extends Reducer<IntWritable, IntWritable,IntWritable,IntWritable> {
    private static int countSum;
    private static int countNum;
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{


        int count = 0;
        for (IntWritable value:values)
        {
            count += value.get();
        }


        context.write(key, new IntWritable(count));

    }
}
