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
 * Effettua la maggior parte della computazione
 *
 * - Input  => (email, elenco_password_associate_alla_email[])
 * - Output => (#password, media_Edit_Distances_normalizzate) ogni output è riferito ad una diversa email
 *
 * - i casi in cui si trova una sola password non sono significativi quindi vengono scartati
 */
public class Reducer1 extends MapReduceBase implements Reducer<Text, Text, IntWritable, DoubleWritable > {
    //todo controlla la posizione di queste variabili
    private IntWritable nPsw = new IntWritable(); //numero di password associate alla email
    private DoubleWritable avgEDN = new DoubleWritable(); //media delle edit distance normalizzate delle password

    public void reduce(Text key, Iterator<Text> values, OutputCollector<IntWritable, DoubleWritable> output,
                       Reporter reporter) throws IOException {

        //creo una lista contenente tutte le password restituite da Iterator
        List<String> passwords = AvgEditDistanceComputer.textIteratorToList(values);

        //ha senso produrre un risultato solo nel caso in cui siano presenti almeno due psw da confrontare
        if(passwords.size() >= 2){
            //imposto il numero delle password
            nPsw.set(passwords.size());
            //calcolo la media delle edit distance normalizzate sulle password (vedi classe AvgEditDistanceComputer)
            avgEDN.set(AvgEditDistanceComputer.computeAvgEditDistance(passwords));
            //restituisco il risultato
            output.collect(nPsw, avgEDN);
        } else {
            //non produco alcun risultato
            System.out.println("n° password " + passwords.size() +" <= 1");
        }
    }
}