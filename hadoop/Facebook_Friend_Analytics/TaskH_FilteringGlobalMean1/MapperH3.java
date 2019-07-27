import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;


public class MapperH3 extends Mapper<Text, Text, IntWritable, Text> {

    String avgString="0";
    public void setup(Context context){
        Configuration conf3 = context.getConfiguration();
        try {

            FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf3);
            FSDataInputStream inputStream = hdfs.open(new Path("/user/ds503/project1/TaskH/job2Tmp/part-r-00000"));

            BufferedReader Reader = new BufferedReader(new InputStreamReader(inputStream));
            avgString  = Reader.readLine();
//            String items[] = line1.split("\t");
//            avgString = items[1];
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException{

        int id = Integer.parseInt(key.toString());
        int count = Integer.parseInt(value.toString());
        double totalAvg = Double.parseDouble(avgString);

        if(count > totalAvg){
            context.write(new IntWritable(id), new Text(count + "," + totalAvg));
        }
    }

}