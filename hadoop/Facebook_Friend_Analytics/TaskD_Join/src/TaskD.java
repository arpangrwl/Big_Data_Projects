import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TaskD {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskD");
        job.setJarByClass(TaskD.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,MapperFriends.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,MapperMyPage.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //job.setCombinerClass(Reducer.class);
        job.setReducerClass(ReducerD.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);


        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}