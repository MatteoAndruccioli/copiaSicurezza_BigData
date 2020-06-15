package prova_progetto1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class Mapper1 extends MapReduceBase
// non sono sicuro che longWritable sia giusto, bisogna vedere cosa indica: credo stia per la riga => dovrebbe esser giusto
// simile ad AvgTemperatureMapper
implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
        //tiro su una linea del documento
        String line = value.toString();
        String[] result = line.split(":");
        System.out.println("("+ result[0]+","+ result[1] +")");
        output.collect(new Text(result[0]), new Text(result[1]));
    }
}