package prova_multijob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class Reducer1 extends MapReduceBase implements Reducer<Text, Text, IntWritable, Text > {
    private IntWritable result = new IntWritable(); //occhio a dove deve stare questo

    public void reduce(Text key, Iterator<Text> values, OutputCollector<IntWritable, Text> output,
                       Reporter reporter) throws IOException {


        int count = 0;
        while (values.hasNext()) {
            //todo occhio se togli sta print a chiamare comunque values.next() se no è un ciclo infinito
            System.out.println(values.next());
            count++;
        }

        System.out.println("("+ count +","+ key +")");

        result.set(count);

        if (count > 1){
            output.collect(result, key);
        } else {
            System.out.println("count " + count +" <= 1");
        }
    }
}