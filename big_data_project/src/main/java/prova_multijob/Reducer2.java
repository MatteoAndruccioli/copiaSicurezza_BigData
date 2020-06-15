package prova_multijob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class Reducer2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
    private Text result = new Text();

    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output,
                       Reporter reporter) throws IOException {


        String temp = "";
        while (values.hasNext()) {
            temp = temp+values.next()+" ";
        }

        System.out.println("("+ key +","+ temp +")");

        result.set(temp);

        output.collect(key, result);
    }
}