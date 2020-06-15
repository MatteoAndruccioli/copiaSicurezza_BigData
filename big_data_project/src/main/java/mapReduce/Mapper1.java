package mapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

//si occupa di recuperare coppie (email,password) dal documento e passarle a Reducer1
public class Mapper1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
        //leggo una linea del documento
        String line = value.toString();
        //le linee 'corrette' sono formattate come segue: 'email:password'
        String[] result = line.split(":");

        /**
         * le linee 'corrette' produrranno solo 2 token:
         *  - result[0] ==> email
         *  - result[1] ==> password
         *
         *  il contenuto delle linee 'corrette' verrà passato al reducer
         */
        if (result.length == 2) {
            //System.out.println("("+ result[0]+","+ result[1] +")");
            output.collect(new Text(result[0]), new Text(result[1]));
        } else {
            System.out.println("!!! errore con linea: " + value.toString()+ " result.length = " + result.length + " != 2");
        }
    }
}