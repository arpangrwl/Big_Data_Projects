import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Random;


public class KMeans {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        int k = Integer.parseInt(args[0]);
        int dimLen = Integer.parseInt(args[1]);


        int iteration = 1;
        while(iteration <= 20) {


            if(iteration!=1) {
                Path centroidDiffPath = new Path("/user/ds503/project2/problem3/output/iter" + (iteration - 1) + "/oldNewCentroidDistanceSum.csv");
                double centroidDiff = readCentroidDifference(centroidDiffPath);
                if (centroidDiff < 0.05) {
                    System.out.println("Successfully converge.");
                    break;
                }
            }

            Configuration conf = new Configuration();
            // Global Variable
            String centroid;
            if(iteration == 1) {
                // init centroid
                Path initCentroidPath = new Path("/user/ds503/project2/problem3/output/initCentroid.csv");
                initCentroid(k, dimLen, initCentroidPath);
                centroid = readCentroid(initCentroidPath);
            }else{
                centroid = readCentroid(new Path("/user/ds503/project2/problem3/output/iter" + (iteration-1)+"/part-r-00000"));
            }

            conf.set("centroid", centroid);
            //k = number of clusters
            conf.set("k", String.valueOf(k));
            //dimLem = number of dimension for each point
            conf.set("dimLen", String.valueOf(dimLen));
            //i
            conf.set("iteration", String.valueOf(iteration));


            Job job = Job.getInstance(conf, "KMeans_iter" + iteration);
            job.setJarByClass(KMeans.class);
            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);
            job.setCombinerClass(Combiner1.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);


            FileInputFormat.addInputPath(job, new Path("/user/ds503/project2/problem3/features.csv"));
            FileOutputFormat.setOutputPath(job, new Path("/user/ds503/project2/problem3/output/iter" + iteration));



            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }

            if (job.isSuccessful()){
                System.out.println("Iteration " + iteration + " is completed.");
            }else{
                System.out.println("Error in job...");
            }
            iteration++;
        }


        // --------  Map2 Job -------
        //Job2 Output the points with corresponding centroid(cluster)
        Configuration conf = new Configuration();
        String finalCentroid = readCentroid(new Path("/user/ds503/project2/problem3/output/iter" + (iteration-1)+"/part-r-00000"));

        conf.set("centroid", finalCentroid);
        //k = number of clusters
        conf.set("k", String.valueOf(k));
        //dimLem = number of dimension for each point
        conf.set("dimLen", String.valueOf(dimLen));
        //i
        conf.set("iteration", String.valueOf(iteration));

        Job job2 = Job.getInstance(conf, "KMeans_Point_Cluster");
        job2.setJarByClass(KMeans.class);
        job2.setMapperClass(Mapper2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path("/user/ds503/project2/problem3/features.csv"));
        FileOutputFormat.setOutputPath(job2, new Path("/user/ds503/project2/problem3/output/FinalOutput"));


        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        if (job2.isSuccessful()){
            System.out.println("Job is Completed Successfully");
        }else{
            System.out.println("Error in job...");
        }

    }


    public static String readCentroid(Path path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
        FSDataInputStream inputStream = hdfs.open(path);
        BufferedReader Reader = new BufferedReader(new InputStreamReader(inputStream));

        StringBuilder sb = new StringBuilder();
        String oneLine = null;
        while((oneLine = Reader.readLine()) != null){
            sb.append(oneLine);
            sb.append("\n");
        }
        return sb.toString().trim();
    }


    public static void initCentroid(int k, int dimLen, Path outputPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
        FSDataOutputStream outputStream = hdfs.create(outputPath);
        StringBuilder sb = new StringBuilder();
        Random rand = new Random();
        for(int i = 0; i< k; i++){
            for(int j = 0; j < dimLen; j++){
                Double randomValue = 5 + 1 * rand.nextDouble();
                sb.append(randomValue);
                if(j != dimLen-1) sb.append(",");
            }
            sb.append("\n");
        }
        outputStream.writeBytes(sb.toString().trim());
        outputStream.close();
    }

    public static double readCentroidDifference(Path path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
        FSDataInputStream inputStream = hdfs.open(path);
        BufferedReader Reader = new BufferedReader(new InputStreamReader(inputStream));

        Double centroidDifference = Double.parseDouble(Reader.readLine());
        return centroidDifference;
    }


}

    