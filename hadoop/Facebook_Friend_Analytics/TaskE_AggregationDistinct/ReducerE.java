import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ReducerE extends Reducer<IntWritable, Text, IntWritable, Text> {
    private IntWritable reduceOutPutKey = new IntWritable();
    private Text reduceOutPutValue = new Text();
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        List<String> valuesArr = new ArrayList<String>();
        for(Text v : values) {
            valuesArr.add(v.toString());
        }
        Set<String> valuesSet = new HashSet<String>(valuesArr);


        int accessCnt = valuesArr.size();
        int uniqueAccessCnt = valuesSet.size();

        reduceOutPutKey = key;
        reduceOutPutValue.set(accessCnt + "\t" + uniqueAccessCnt);

        context.write(reduceOutPutKey,reduceOutPutValue);


    }
}
