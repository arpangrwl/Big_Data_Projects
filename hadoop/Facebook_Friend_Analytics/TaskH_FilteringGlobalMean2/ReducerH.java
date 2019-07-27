import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import java.util.*;

public class ReducerH extends Reducer<IntWritable, IntWritable,IntWritable,Text> {
    private int countSum = 0;
    private int countNum = 0;
    //private Map<String,Integer> map=new HashMap<String, Integer>();
    private static Map<Integer, String> map;

    protected void setup(Context context) throws IOException, InterruptedException {
        map = new HashMap<>();
    }

    public void cleanup(Reducer<IntWritable, IntWritable,IntWritable,Text>.Context context)throws IOException, InterruptedException {

        //map.entrySet() to list
        List<Map.Entry<Integer, String>> list = new LinkedList<Map.Entry<Integer, String>>(map.entrySet());
        //sort list by comparator; 4-3-2-1
        Collections.sort(list, new Comparator<Map.Entry<Integer, String>>() {
            @Override
            public int compare(Map.Entry<Integer, String> arg0, Map.Entry<Integer, String> arg1) {
                int avg0 = Integer.parseInt(arg0.getValue().split(",")[1]);
                int avg1 = Integer.parseInt(arg1.getValue().split(",")[1]);
                return (int) (avg1-avg0);
            }
        });
        float totalAvgFriendCount = Float.parseFloat(list.get(0).getValue().split(",")[3]);

        for (int i = 0; i < list.size(); i++) {
            int friendsCount = Integer.parseInt(list.get(i).getValue().split(",")[0]);
            if(friendsCount > totalAvgFriendCount){
                context.write(new IntWritable(list.get(i).getKey()), new Text(friendsCount + "," + totalAvgFriendCount));
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

        map.put(id, (count + "," + countNum + "," +countSum + "," +avg));
        //context.write(key, new Text(count + "," + countNum + "," +countSum + "," +avg));

    }





}
