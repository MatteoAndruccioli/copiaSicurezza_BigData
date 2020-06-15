package mapReduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * questo mapper si occupa solo di estrarre da un file le informazioni prodotte da Reducer1, e
 * renderle disponibili a Reducer2, senza modificarle
 */
public class Mapper2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    //todo controlla la posizione di queste variabili
    private IntWritable nPsw = new IntWritable(); //numero di password associate alla email
    private DoubleWritable avgEDN = new DoubleWritable(); //media delle edit distance normalizzate delle password

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
        IntWritable npswd = new IntWritable();

        //estraggo la linea dal documento
        String line = value.toString();
        //separo la chiave dal valore
        String[] result = line.split("\\s");

        //ricostruisco i valori numerici a partire dalle stringhe trovate
        int nPsw_found = Integer.parseInt(result[0].trim());
        double avgEDN_found = Double.parseDouble(result[1].trim());

        //setto i valori che devo restituire
        nPsw.set(nPsw_found);
        avgEDN.set(avgEDN_found);

        //passo i valori al Reducer, in questo modo avrò una aggregazione sul 'numero di password per mail'
        System.out.println("("+ nPsw_found +","+ avgEDN_found +")");
        output.collect(nPsw, avgEDN);
    }
}