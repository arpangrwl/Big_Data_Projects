import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskH2 {

    public static void main(String[] args) throws Exception {

        if(args.length !=2){
            System.err.println("Invalid Command");
            System.err.println("Usage: TaskH <input path> <output path>");
            System.exit(0);
        }

        // --------- first Map1-Reduce1 Job -------------------------
        //Job1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "TaskH1");

        job1.setJarByClass(TaskH2.class); //driver
        job1.setMapperClass(MapperH.class);
        job1.setReducerClass(ReducerH.class);

        job1.setMapOutputKeyClass(IntWritable.class);          // mapper1 output
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(IntWritable.class);      // reducer1 output
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

    }
}
