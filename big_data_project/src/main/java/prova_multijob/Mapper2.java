package prova_multijob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class Mapper2 extends MapReduceBase
        implements Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(LongWritable key, Text value, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException {
        IntWritable npswd = new IntWritable();

        //tiro su una linea del documento
        String line = value.toString();
        String[] result = line.split("\\s");

        int val = Integer.parseInt(result[0].trim());
        npswd.set(val);
        System.out.println("("+ val +","+ result[1] +")");
        output.collect(npswd, new Text(result[1]));
    }
}
