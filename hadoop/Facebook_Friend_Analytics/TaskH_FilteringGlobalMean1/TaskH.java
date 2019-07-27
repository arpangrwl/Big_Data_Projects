import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaskH {

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

        job1.setJarByClass(TaskH.class); //driver
        job1.setMapperClass(MapperH1.class);
        job1.setReducerClass(ReducerH1.class);

        job1.setOutputKeyClass(IntWritable.class);      // reducer1 output
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/job1Tmp"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // -------- Second Map2 Reduce2 Job ------- compute average
        //Job2
        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

        Job job2 = Job.getInstance(conf2, "TaskH2");

        job2.setJarByClass(TaskH.class); //driver
        job2.setMapperClass(MapperH2.class);
        job2.setReducerClass(ReducerH2.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapOutputKeyClass(IntWritable.class);   // reducer2 output
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setOutputKeyClass(Text.class);      // reducer2 output
        job2.setOutputValueClass(NullWritable.class);

        //job2.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job2, new Path(args[1] + "/job1Tmp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/job2Tmp"));


        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }


        // -------- Third Map3 Reduce3 Job ------- compute average
        //Job3
        Configuration conf3 = new Configuration();
        conf3.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

        Job job3 = Job.getInstance(conf3, "TaskH3");

        job3.setJarByClass(TaskH.class); //driver
        job3.setMapperClass(MapperH3.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);

        job3.setMapOutputKeyClass(IntWritable.class);      // reducer2 output
        job3.setMapOutputValueClass(Text.class);
        job3.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job3, new Path(args[1] + "/job1Tmp"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]+"/job3Final"));


        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }
        //---
        if (job3.isSuccessful()){
            System.out.println("Job is Completed Successfully");
        }else{
            System.out.println("Error in job...");
        }

    }
}
