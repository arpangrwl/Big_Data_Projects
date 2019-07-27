import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TaskC {



    public static void main(String[] args) throws Exception {

        if(args.length !=2){
            System.err.println("Invalid Command");
            System.err.println("Usage: TaskA <input path> <output path>");
            System.exit(0);
        }

        // --------- first Map1-Reduce1 Job -------------------------
        //Job1
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "TaskC1");

        job1.setJarByClass(TaskC.class); //driver
        job1.setMapperClass(MapperC1.class);
        job1.setReducerClass(ReducerC1.class);
        //job1.setCombinerClass(ReducerC1.class);

        job1.setOutputKeyClass(Text.class);      // reducer1 output
        job1.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/job1Tmp"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // -------- Second Map2 Reduce3 Job -------
        //Job2
        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

        Job job2 = Job.getInstance(conf2, "TaskC2");

        job2.setJarByClass(TaskC.class); //driver
        job2.setMapperClass(MapperC2.class);
        job2.setReducerClass(ReducerC2.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);      // reducer2 output
        job2.setOutputValueClass(IntWritable.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setSortComparatorClass(IntComparator.class);
        job2.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job2, new Path(args[1] + "/job1Tmp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/job2Final"));


        //job2.setSortComparatorClass(IntComparator.class)
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
        if (job2.isSuccessful()){
            System.out.println("Job is Completed Successfully");
        }else{
            System.out.println("Error in job...");
        }

    }
}
