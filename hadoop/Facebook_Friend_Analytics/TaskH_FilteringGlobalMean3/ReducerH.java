import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import java.util.*;

public class ReducerH extends Reducer<IntWritable, IntWritable,IntWritable,Text> {
    private int countSum = 0;
    private int countNum = 0;
    private float totalAvg = 0;
    private Stack<String> stack = new Stack<>();

    public void cleanup(Reducer<IntWritable, IntWritable,IntWritable,Text>.Context context)throws IOException, InterruptedException {
        while(!stack.isEmpty()){
            String[] onePerson = stack.pop().split(",");
            int personID = Integer.parseInt(onePerson[0]);
            int friendCount = Integer.parseInt(onePerson[1]);
            if(totalAvg==0){
                totalAvg = Float.parseFloat(onePerson[2]);
            }
            if(friendCount > totalAvg){
                context.write(new IntWritable(personID), new Text("" + friendCount + "," + totalAvg));
            }

        }
    }

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        int count = 0;
        for (IntWritable value:values)
        {
            count += value.get();
        }
        countSum+=count;
        countNum+=1;
        float avg = (float) countSum/countNum;
        int id = key.get();


        stack.push(id + "," + count + "," + avg);
    }





}
