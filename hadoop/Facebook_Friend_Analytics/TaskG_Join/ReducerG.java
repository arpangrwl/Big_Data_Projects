import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class ReducerG extends Reducer<IntWritable, Text, IntWritable, Text> {
    private IntWritable reduceOutPutKey = new IntWritable();
    private Text reduceOutPutValue = new Text();
    //private Text reduceOutPutValue = new Text();
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        Set<Integer> friendsSet = new TreeSet<Integer>();
        Set<Integer> accessSet = new TreeSet<Integer>();


        for(Text v : values) {

            String[] valueList = v.toString().split("-");
            String fileSource = valueList[0];
            int id = Integer.parseInt(valueList[1]);

            if (fileSource.contains("Friends")){
                friendsSet.add(id);
            }
            if (fileSource.contains("AccessLog")){
                accessSet.add(id);
            }
        }

        Set<Integer> difference = new TreeSet<>(friendsSet);
        difference.removeAll(accessSet);

        int friendNum = friendsSet.size();
        int loseInterestNum = difference.size();

        if (loseInterestNum > 0){


            reduceOutPutKey = key;
            reduceOutPutValue.set(friendNum + "," + loseInterestNum);
            context.write(reduceOutPutKey, reduceOutPutValue);
        }



    }
}
