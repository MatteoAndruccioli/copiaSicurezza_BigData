package mapReduceTest;

import mapReduce.AvgEditDistanceComputer;
import mapReduce.LevenshteinDistance;
import mapReduce.Pair;
import mapReduce.PairList;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class TestAEDComputer {

    //testa funzionamento AvgEditDistanceComputer
    private static void testGenerico(){

        System.out.println("Calcolo le coppie: [(\"gianna\", \"domenica\"), (\"gianna\", \"volpe\"), (\"domenica\", \"volpe\")]");

        //todo: should return [("gianna", "domenica"), ("gianna", "volpe"), ("domenica", "volpe")]
        List<String> temp = Arrays.asList("gianna", "domenica", "volpe");
        PairList pl = new PairList();
        List<Pair> pairs = pl.findAllPairs(temp);
        System.out.println(pairs);



        System.out.println("\n\n//-------------------------------------------------------------------------");
        System.out.println("Check compute editDistance");

        //todo: check compute editDistance
        List<Integer> eds = new LinkedList<>();
        for(int i=0; i<pairs.size(); i++){
            Pair p = pairs.get(i);
            eds.add(LevenshteinDistance.minDistance(p.getX(),p.getY()));
            System.out.println(p.toString() + " ==> " + LevenshteinDistance.minDistance(p.getX(),p.getY()));
        }



        System.out.println("\n\n//-------------------------------------------------------------------------");
        System.out.println("Check maxEditDistance");

        //todo: check compute maxEditDistance
        List<Integer> maxEDs = new LinkedList<>();
        for(int i=0; i<pairs.size(); i++){
            Pair p = pairs.get(i);
            maxEDs.add(AvgEditDistanceComputer.maxLength(p.getX(),p.getY()));
            System.out.println(p.toString() + " ==> " + AvgEditDistanceComputer.maxLength(p.getX(),p.getY()));
        }





        System.out.println("\n\n//-------------------------------------------------------------------------");
        System.out.println("Check editDistanceNormalizer");

        //todo: check editDistanceNormalizer
        List<Double> edns = new LinkedList<>();
        for(int i=0; i<pairs.size(); i++){
            Pair p = pairs.get(i);
            edns.add((double)eds.get(i)/maxEDs.get(i));
            System.out.println(p.toString() + " ==> "
                    + AvgEditDistanceComputer.editDistanceNormalizer(eds.get(i),maxEDs.get(i)) +
                    " == " + (double)eds.get(i)/maxEDs.get(i));
        }



        System.out.println("\n\n//-------------------------------------------------------------------------");
        System.out.println("Check computeAvg");

        //todo: check computeAvg
        System.out.println(edns);
        System.out.println(" Media degli ED normalizzati: " + AvgEditDistanceComputer.computeAvg(edns) + "deve essere uguale a: ");
        System.out.println(" risultato computeAvgEditDistance(List<String> allPsw): " + AvgEditDistanceComputer.computeAvgEditDistance(temp));


    }

    //utilizzo questa funzione per provare 'a mano' risultati su piccolo gruppo di elementi
    private static double testCampioneDiProva(List<String> passwords){
        double avg = AvgEditDistanceComputer.computeAvgEditDistance(passwords);
        System.out.println(passwords.toString() + " avg " + avg);
        return avg;
    }

    //gruppo 2
    private static void testCampioneDiProva2(){
        double avg1 = testCampioneDiProva(Arrays.asList("65416cdd","6541dd"));
        double avg2 = testCampioneDiProva(Arrays.asList("ef31655","ef31655ffr"));
        double avg3 = testCampioneDiProva(Arrays.asList("ewed6656","ewed6656"));

        System.out.println("Media tra i valori (output per 2): "+ AvgEditDistanceComputer.computeAvg(Arrays.asList(avg1,avg2,avg3)));
    }

    //gruppo 3
    private static void testCampioneDiProva3(){
        double avg1 = testCampioneDiProva(Arrays.asList("esrbyuewedhbc","esrbyuedhbc","esrbyuewedh"));
        double avg2 = testCampioneDiProva(Arrays.asList("eieon351666de","eieon35666de","eieon351666def"));
        double avg3 = testCampioneDiProva(Arrays.asList("bediend651533","bed","beercjknec77"));
        double avg4 = testCampioneDiProva(Arrays.asList("eavefarc65152","eavqeqewdew52","wrevrfni72566"));

        System.out.println("Media tra i valori (output per 3): "+ AvgEditDistanceComputer.computeAvg(Arrays.asList(avg1,avg2,avg3,avg4)));
    }

    //gruppo 4
    private static void testCampioneDiProva4(){
        double avg1 = testCampioneDiProva(Arrays.asList("a1516116","wererfecerfcrwbtht","escwer3151@#1162","5gytgyttgggty"));
        double avg2 = testCampioneDiProva(Arrays.asList("ede6161","f","ioiumikynujn351356183","refr15156rttr"));

        System.out.println("Media tra i valori (output per 4): "+ AvgEditDistanceComputer.computeAvg(Arrays.asList(avg1,avg2)));
    }


    public static void main(String[] args) throws IOException {

        //testGenerico();

        //test in riferimento agli imput di prova (valori riportati in psw1.txt - file nella root)
        testCampioneDiProva2();
        testCampioneDiProva3();
        testCampioneDiProva4();

    }
}
