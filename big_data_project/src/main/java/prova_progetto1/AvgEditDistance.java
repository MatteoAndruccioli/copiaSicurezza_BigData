package prova_progetto1;

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

// hadoop jar big_data_project.jar prova_progetto1.AvgEditDistance /bigdata/dataset/projectTest mapreduce/projectTest/output
public class AvgEditDistance {
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(AvgEditDistance.class);
        conf.setJobName("Avg Edit Distance");

        Path inputPath = new Path(args[0]),
                outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());

        if(fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, outputPath);

        conf.setMapperClass(Mapper1.class);
        conf.setReducerClass(Reducer1.class);

        if(args.length>2 && Integer.parseInt(args[2])>=0){
            conf.setNumReduceTasks(Integer.parseInt(args[2]));
        }
        else{
            conf.setNumReduceTasks(1);
        }

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
    }
}
