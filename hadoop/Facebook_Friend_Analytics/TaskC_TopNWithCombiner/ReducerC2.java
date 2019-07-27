import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ReducerC2 extends Reducer<IntWritable, Text, Text,IntWritable> {
    int cnt = 0;
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        //count:[id1,id2]

        for (Text value: values){

            cnt += 1;
            if(cnt <= 10){
                context.write(value, key);
            }
        }




    }
}
