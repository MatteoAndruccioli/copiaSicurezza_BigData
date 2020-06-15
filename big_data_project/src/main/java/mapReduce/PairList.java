package mapReduce;

import java.util.LinkedList;
import java.util.List;

public class PairList {
    /**
     * restituisce una lista di tutte le coppie Pair(head,elem)
     * in cui elem appartienea words ed è a un indice maggiore
     */
    public List<Pair> findPairsOfHead(List<String> words) {
        List<Pair> list = new LinkedList<>();
        if (words.size() >= 2) {
            List<String> temp = words.subList(1,words.size());
            for (int i = 0; i < temp.size(); i++){
                list.add(new Pair(words.get(0), temp.get(i)));
            }
        }
        return list;
    }

    public List<Pair> findAllPairs(List<String> passwords) {
        List<Pair> temp = new LinkedList<>();
        if (passwords.size() >= 2) {
            temp.addAll(findPairsOfHead(new LinkedList<String>(passwords)));
            temp.addAll(findAllPairs(new LinkedList<String>(passwords.subList(1,passwords.size()))));
        }
        return temp;
    }
}
