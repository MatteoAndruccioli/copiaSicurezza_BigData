package mapReduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class AvgEditDistanceComputer {

    //todo: devi solo chiamare una delle due versioni di computeAvgEditDistance

    //voglio un Iterator<Text> che mi restituisca tutte le passwords (non utilizzato)
    public static double computeAvgEditDistance(Iterator<Text> values) {
        //prendo tutte le psw raccolte in input dal reducer1 e le inserisco in una lista
        return computeAvgEditDistance(textIteratorToList(values));
    }


    //voglio una List<String> contenente tutte le passwords
    public static double computeAvgEditDistance(List<String> allPsw) {
        //calcolo tutte le coppie possibili di psw che dovranno essere confrontate
        PairList pl = new PairList();
        List<Pair> pswPairs = pl.findAllPairs(allPsw);

        //per ogni coppia calcolo l'Edit Distance, la normalizzo e memorizzo il tutto in una lista

        List<Double> normalizedEDs = new LinkedList<>();

        for (int i=0; i<pswPairs.size(); i++){
            normalizedEDs.add( computeEditDistance(pswPairs.get(i)));
        }

        return computeAvg(normalizedEDs); //todo
    }

    //calcola la media della somma di una lista di double
    public static double computeAvg(List<Double> nums) {
        double result = 0.0;
        for (int i=0; i<nums.size(); i++){
            result += nums.get(i);
        }
        return nums.isEmpty() ? 0.0 :  (result/nums.size());
    }

    //data una coppia di string calcola l'edit distance normalizzato
    public static double computeEditDistance(Pair p){
        return editDistanceNormalizer(LevenshteinDistance.minDistance(p.getX(), p.getY()), maxLength(p.getX(), p.getY()));
    }


    //a partire da Iterator<Text> ottengo una List<string> su cui è molto piu facile compiere trasformazioni
    public static List<String> textIteratorToList(Iterator<Text> values) {
        List<String> list = new LinkedList<>();

        while (values.hasNext()) {
            list.add(values.next().toString());
        }

        System.out.println("textIteratorToList: " + list.toString());

        return list;
    }

    //a partire da Iterator<Double> ottengo una List<Double> su cui è molto piu facile compiere trasformazioni
    public static List<Double> doubleIteratorToList(Iterator<DoubleWritable> values) {
        List<Double> list = new LinkedList<>();

        while (values.hasNext()) {
            list.add(values.next().get());
        }

        return list;
    }

    //poichè abbiamo bisogno di confrontare editdistance di password con lunghezze differenti è necessario normalizzare
    public static double editDistanceNormalizer(int editDistance, int maxEditDistance) {
        return (double)editDistance/maxEditDistance;
    }

    //ritorna la lunghezza della parola più lunga tra le due confrontate
    public static int maxLength(String word1, String word2) {
        return word1.length() > word2.length() ? word1.length() : word2.length();
    }
}


