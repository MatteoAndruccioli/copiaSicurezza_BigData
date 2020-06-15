import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._
import Utils._

// export HADOOP_USER_NAME=hdfs
// spark-submit --class Progetto BD-301-spark-basics.jar
object Progetto extends App{

  /*
  //calcola la media, l'ho dovuta aggiungere perchè la funzione java mi dava problemi
  def avg(list:List[Double]): Double = list.size match {
    case 0 => 0.0
    case _ => list.fold(0.0)(_+_)/list.size
  }
   */

  // Function to parse mail:password records;
  def parseLine(line: String): (String, String) = {
    val result:Array[String] = line.split(":");
    result.length match {
      case 2 => (result(0), result(1))
      case _ => ("","")
    }
  }

  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Grado di variabilità medio edit distance password")
    val sc = new SparkContext(conf)

    // Create an RDD from the files in the given folder
    val rddPsw = sc.textFile("hdfs:/bigdata/dataset/projectSparkTest")

    /**
     * - riduco numero di partizioni con coalesce, forse potresti omettere coalesce iniziale
     * - ottengo un rdd chiave-valore dove la chiave sarà la mail e il valore sarà la password
     * - prendo in considerazione solo quei valori per cui nè chiave nè valore sono stringhe vuote
     * - raggruppo sulle mail, quindi avrò alla fine (mail,psw[])
     */
    val rddMailPswsKv = rddPsw
      .coalesce(2)
      .map(x => parseLine(x))
      .filter(x => !(x._1=="" || x._2==""))
      .groupByKey()

    /**
     * - la prima map trasforma il valore da un'iterator a una lista, questo mi tornerà utile nelle 2 successive operazioni
     * - elimino tutte quelle coppie di cui conosco solo una password filtrando
     * - trasformo l'rdd (#NumeroPassword,Media(edit_distance))
     * - raggruppo sulla chiave, così da ottenere un rdd (#NumeroPassword, Media(edit_distance)[])
     */
    val rddNPswsAvgEDKv = rddMailPswsKv
      .mapValues(x => x.toList)
      .filter(x => x._2.length > 1)
      .map({case(_,v)=>(v.length, computeAvgEditDistance(v))})
      .groupByKey()

    /**
     * - ottengo grado di variabilità medio andando a effettuare una media sui gradi di variabilità calcolati
     * - ottengo (#NumeroPassword, Media(Media(edit_distance))
     */
    val rddNPswsAvgVD = rddNPswsAvgEDKv.mapValues(x => computeAvg(x.toList))

    //Save the RDD on HDFS; the directory should NOT exist
    rddNPswsAvgVD.saveAsTextFile("hdfs:/user/matteo/esameSpark/")
  }
}
