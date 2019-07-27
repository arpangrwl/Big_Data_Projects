import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Reducer1 extends Reducer<IntWritable, IntWritable,IntWritable,IntWritable> {
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
