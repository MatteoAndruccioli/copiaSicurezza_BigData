package mapReduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 *  - Input     => (#password, elenco_medie_Edit_Distances_normalizzate[])
 *  - Output    => (#password, grado di variabilità medio)
 *
 *  grado di variabilità medio == media_per_stesso_numero_psw(media_per_stessa_email_delle_Edit_Distances_normalizzate)
 *
 *  fondamentalmente per ogni elenco in input effettua una media sui valori dell'elenco
 */
public class Reducer2 extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    private DoubleWritable avg = new DoubleWritable(); //grado di variabilità medio

    public void reduce(IntWritable key, Iterator<DoubleWritable> values, OutputCollector<IntWritable, DoubleWritable> output,
                       Reporter reporter) throws IOException {

        //creo una lista dei valori prodotti dall'iterable (EDN medie)
        List<Double> avgEDNs = AvgEditDistanceComputer.doubleIteratorToList(values);

        //calcolo la media tra i valori dell'elenco in input ottendendo così il 'grado di variabilità medio'
        avg.set(AvgEditDistanceComputer.computeAvg(avgEDNs));

        System.out.println("("+ key +","+ avg.get() +")");

        output.collect(key, avg);
    }
}