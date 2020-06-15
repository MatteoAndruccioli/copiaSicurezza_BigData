package prova_multijob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class Mapper1 extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
        //tiro su una linea del documento
        String line = value.toString();
        String[] result = line.split(":");
        if (result.length == 2) {
            //System.out.println("("+ result[0]+","+ result[1] +")");
            output.collect(new Text(result[0]), new Text(result[1]));
        } else {
            System.out.println("!!! errore con linea: " + value.toString()+ " result.length = " + result.length + " != 2");
        }

    }
}