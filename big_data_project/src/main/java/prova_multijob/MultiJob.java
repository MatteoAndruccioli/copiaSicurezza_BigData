package prova_multijob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

// hadoop jar big_data_project.jar prova_multijob.MultiJob /bigdata/dataset/projectTest mapreduce/multiJobTest/temp mapreduce/multiJobTest/output
public class MultiJob {
    public static void main(String[] args) throws IOException {
        JobConf conf1 = new JobConf(MultiJob.class);
        conf1.setJobName("Multi job test");

        Path inputPath1 = new Path(args[0]),
                outputPath1 = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());

        if(fs.exists(outputPath1)) {
            fs.delete(outputPath1, true);
        }

        FileInputFormat.addInputPath(conf1, inputPath1);
        FileOutputFormat.setOutputPath(conf1, outputPath1);

        conf1.setMapperClass(Mapper1.class);
        conf1.setReducerClass(Reducer1.class);

        if(args.length>3 && Integer.parseInt(args[3])>=0){
            conf1.setNumReduceTasks(Integer.parseInt(args[3]));
        }
        else{
            conf1.setNumReduceTasks(1);
        }

        conf1.setMapOutputKeyClass(Text.class);
        conf1.setMapOutputValueClass(Text.class);
        conf1.setOutputKeyClass(IntWritable.class);
        conf1.setOutputValueClass(Text.class);

        JobClient.runJob(conf1);


        //-----------------------------------------------------------------------
        JobConf conf2 = new JobConf(MultiJob.class);
        conf2.setJobName("Multi job test");

        Path inputPath2 = new Path(args[1]),
                outputPath2 = new Path(args[2]);


        if(fs.exists(outputPath2)) {
            fs.delete(outputPath2, true);
        }

        FileInputFormat.addInputPath(conf2, inputPath2);
        FileOutputFormat.setOutputPath(conf2, outputPath2);

        conf2.setMapperClass(Mapper2.class);
        conf2.setReducerClass(Reducer2.class);

        if(args.length>3 && Integer.parseInt(args[3])>=0){
            conf2.setNumReduceTasks(Integer.parseInt(args[3]));
        }
        else{
            conf2.setNumReduceTasks(1);
        }

        conf2.setMapOutputKeyClass(IntWritable.class);
        conf2.setMapOutputValueClass(Text.class);
        conf2.setOutputKeyClass(IntWritable.class);
        conf2.setOutputValueClass(Text.class);

        JobClient.runJob(conf2);
    }
}
