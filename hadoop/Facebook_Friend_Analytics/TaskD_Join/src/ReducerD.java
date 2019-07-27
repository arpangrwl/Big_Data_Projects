import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class ReducerD extends Reducer<IntWritable, Text,IntWritable,Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        String name = "";
        int count = 0;

        for (Text value: values){
            String[] valueList = value.toString().split(",");
            String fileSource = valueList[0];
            //int friendCount = Integer.parseInt(valueList[1]);

            if(fileSource.contains("Name")){
                name = valueList[1];
            }else if(fileSource.contains("Friend")){
                count += Integer.parseInt(valueList[1]);
            }
        }

        context.write(key, new Text(name + "," + count));

    }
}
