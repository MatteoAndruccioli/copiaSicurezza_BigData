
object Pairs {
  case class Pair(x:String, y:String)

  //restituisce tutte le coppie del primo elemento con ogni altro elemento
  def findPairsOfHead(words: List[String]):List[Pair] = {
    words.tail.toStream.map(tailElem => Pair(words.head, tailElem)).toList
  }

  //restituisce tutte le possibili coppie, senza considerare l'ordine degli elementi interni alle coppie
  //ovvero se ho ("w1","w2") NON avrò ("w2","w1") => non ho ripetizione
  def findAllPairs(words: List[String]):List[Pair] = words.size match {
    case n if n>=2 => findPairsOfHead(words) ::: findAllPairs(words.tail) //::: concatena le due liste
    case _ => List()
  }

}

object LevenshteinDistance {
  def minDistance(word1:String, word2:String): Int = {
    val len1: Int = word1.length
    val len2: Int = word2.length

    // len1+1, len2+1, because finally return dp[len1][len2]
    val dp = Array.ofDim[Int](len1+1,len2+1)

    //to include len1
    for( i <- 0 to len1){
      dp(i)(0) = i
    }

    for( j <- 0 to len2){
      dp(0)(j) = j
    }

    //until non comprende l'=
    for( i <- 0 until len1){
      val c1: Char = word1.charAt(i)
      for( j <- 0 until len2){
        val c2: Char = word2.charAt(j)

        //se gli ultimi 2 char sono uguali
        if(c1==c2){
          dp(i+1)(j+1) = dp(i)(j)
        } else {
          val replace: Int = dp(i)(j) + 1
          val insert: Int = dp(i)(j+1) + 1
          val delete: Int = dp(i+1)(j) + 1

          val min = Math.min(delete,Math.min(replace, insert))
          dp(i+1)(j+1) = min
        }
      }
    }
    //valore ritornato
    dp(len1)(len2)
  }
}

object Utils{
  import Pairs._
  import LevenshteinDistance._
  import org.apache.hadoop.io.DoubleWritable
  import org.apache.hadoop.io.Text

  //todo: devi solo chiamare computeAvgEditDistance

  //voglio una List<String> contenente tutte le passwords, ritorno la media delle edit distance
  def computeAvgEditDistance(allPsw: List[String]): Double = {
    //calcolo tutte le coppie possibili di psw che dovranno essere confrontate
    val pswPairs:List[Pair] = findAllPairs(allPsw)
    //per ogni coppia calcolo l'Edit Distance, la normalizzo e memorizzo il tutto in una lista
    val normalizedEDs:List[Double] = pswPairs.toStream.map(p => computeEditDistance(p)).toList
    //calcolo la media delle Edit distances e la restituisco
    computeAvg(normalizedEDs)
  }

  //calcola la media della somma di una lista di double
  def computeAvg(nums :List[Double]):Double = {
    val sum :Double = nums.fold(0.0)(_+_)
    sum/nums.size
  }

  //data una coppia di string calcola l'edit distance normalizzato
  def computeEditDistance(p: Pair) = editDistanceNormalizer(minDistance(p.x, p.y), maxLength(p.x, p.y))

  //a partire da Iterator<Text> ottengo una List<String> su cui è molto piu facile compiere trasformazioni
  def textIteratorToList(values: Iterator[Text]): List[String] = values.toStream.map(e => e.toString()).toList

  //a partire da Iterator<Double> ottengo una List<Double> su cui è molto piu facile compiere trasformazioni
  def doubleIteratorToList(values: Iterator[DoubleWritable]): List[Double] = values.toStream.map(e => e.get()).toList

  //poichè abbiamo bisogno di confrontare editdistance di password con lunghezze differenti è necessario normalizzare
  def editDistanceNormalizer(editDistance: Int, maxEditDistance: Int):Double = editDistance.toDouble/maxEditDistance.toDouble

  //ritorna la lunghezza della parola più lunga tra le due confrontate
  def maxLength(word1:String, word2:String):Int = Math.max(word1.length, word2.length)
}




object TestPairs {
  import Pairs._

  def main(args: Array[String]): Unit = {
    println("findPairsOfHead: ", findPairsOfHead(List("gianna", "domenica", "volpe", "giaguaro")))
    println("findAllPairs: ", findAllPairs(List("gianna", "domenica", "volpe", "giaguaro")))
  }
}

object TestLevenshteinDistance {
  import LevenshteinDistance._

  def main(args: Array[String]): Unit = {
    println("LevenshteinDistance (\"gianna\", \"domenica\"): ", minDistance("gianna", "domenica"))
    println("LevenshteinDistance (\"book\", \"back\"): ", minDistance("book", "back"))
    println("LevenshteinDistance (\"arcibaldo\", \"baldo\"): ", minDistance("arcibaldo", "baldo"))
    println("LevenshteinDistance (\"arcibaldo\", \"arcobaleno\"): ", minDistance("arcibaldo", "arcobaleno"))
  }
}

object  TestUtils {
  import Utils._
  import Pairs._

  def testCampioneDiProva(passwords: List[String]) = {
    val avg:Double = computeAvgEditDistance(passwords)
    System.out.println(passwords.toString() + " avg " + avg)
    avg
  }

  //gruppo 2
  private def testCampioneDiProva2(): Unit = {
    val avg1 = testCampioneDiProva(List("65416cdd", "6541dd"))
    val avg2 = testCampioneDiProva(List("ef31655", "ef31655ffr"))
    val avg3 = testCampioneDiProva(List("ewed6656", "ewed6656"))
    System.out.println("Media tra i valori (output per 2): " + computeAvg(List(avg1, avg2, avg3)))
  }

  //gruppo 3
  private def testCampioneDiProva3(): Unit = {
    val avg1 = testCampioneDiProva(List("esrbyuewedhbc", "esrbyuedhbc", "esrbyuewedh"))
    val avg2 = testCampioneDiProva(List("eieon351666de", "eieon35666de", "eieon351666def"))
    val avg3 = testCampioneDiProva(List("bediend651533", "bed", "beercjknec77"))
    val avg4 = testCampioneDiProva(List("eavefarc65152", "eavqeqewdew52", "wrevrfni72566"))
    System.out.println("Media tra i valori (output per 3): " + computeAvg(List(avg1, avg2, avg3, avg4)))
  }

  //gruppo 4
  private def testCampioneDiProva4(): Unit = {
    val avg1 = testCampioneDiProva(List("a1516116", "wererfecerfcrwbtht", "escwer3151@#1162", "5gytgyttgggty"))
    val avg2 = testCampioneDiProva(List("ede6161", "f", "ioiumikynujn351356183", "refr15156rttr"))
    System.out.println("Media tra i valori (output per 4): " + computeAvg(List(avg1, avg2)))
  }

  def main(args: Array[String]): Unit = {
    /*
    println("maxLength (\"gianna\", \"domenica\"): ", maxLength("gianna", "domenica"))
    println("maxLength (\"book\", \"back\"): ", maxLength("book", "back"))
    println("maxLength (\"arcibaldo\", \"baldo\"): ", maxLength("arcibaldo", "baldo"))

    println("------------------------------------- Edit Distance Normalizer ----------------------------------------------------")

    println("editDistanceNormalizer(8,9): ", editDistanceNormalizer(8,9))


    println("------------------------------------- computeEditDistance ----------------------------------------------------")

    println("computeEditDistance(Pair('gianna','domenica')): ", computeEditDistance(Pair("gianna","domenica"))) //0.75

    println("------------------------------------- computeAvg ----------------------------------------------------")

    println("computeAvg(List(0.85, 0.96, 0.84, 1.57, 1.6)): ", computeAvg(List(0.85, 0.96, 0.84, 1.57, 1.6))) //1.164

     */
    testCampioneDiProva2()
    testCampioneDiProva3()
    testCampioneDiProva4()

  }
}
