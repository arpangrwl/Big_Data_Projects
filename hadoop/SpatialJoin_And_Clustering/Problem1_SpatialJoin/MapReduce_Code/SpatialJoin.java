import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class SpatialJoin {
    public static void main(String[] args) throws Exception {


        if(args.length !=4){
            System.err.println("Invalid Command");
            System.err.println("Usage: SpatialJoin <input path1> <input path2> <output path> <window location>(bottomLeftX,bottonLeftY,topRightX,topRightY)");
            System.exit(0);
        }

        Configuration conf1 = new Configuration();
        conf1.set("windowLocation", args[3]);
        Job job1 = Job.getInstance(conf1, "SpatialJoin");

        job1.setJarByClass(SpatialJoin.class);
        job1.setReducerClass(Reducer1.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);




        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class,MapperPoints.class); //P.csv
        MultipleInputs.addInputPath(job1, new Path(args[1]),TextInputFormat.class,MapperRec.class);     //R.csv
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        if (job1.isSuccessful()){
            System.out.println("Job is Completed Successfully");
        }else{
            System.out.println("Error in job...");
        }


    }
}