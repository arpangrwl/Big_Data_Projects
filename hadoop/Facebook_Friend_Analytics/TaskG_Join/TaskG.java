import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TaskG {

    public static void main(String[] args) throws Exception {

        if(args.length !=3){
            System.err.println("Invalid Command");
            System.err.println("Usage: TaskG <input path> <input path> <output path>");
            System.exit(0);
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ":\t");  // output key value separator
        Job job = Job.getInstance(conf, "TaskG");

        job.setJarByClass(TaskG.class);
        job.setMapperClass(MapperG.class);
        job.setReducerClass(ReducerG.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setSortComparatorClass(IntComparator.class);


        System.exit(job.waitForCompletion(true)?0:1);
    }
}
