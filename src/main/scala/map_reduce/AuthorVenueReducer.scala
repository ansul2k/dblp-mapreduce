package map_reduce

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.LoggerFactory.getLogger

import scala.collection.immutable.ListMap
import scala.collection.mutable.HashMap

object AuthorVenueReducer extends Reducer[Text, Text, Text, IntWritable] {

  override def reduce(key: Text, values: lang.Iterable[Text],
                      context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {

    //calculating number of authors per venue
    val authorCountHMap = new HashMap[String, Int]()
    values.forEach(value => authorCountHMap.put(value.toString,
      authorCountHMap.getOrElse(value.toString,0)+1))

    //sorting and taking top 10 values
    val authorCountHMapSorted = ListMap(authorCountHMap.toSeq.sortWith(_._2 > _._2):_*).take(10)
    authorCountHMapSorted foreach(
      temp => context.write(new Text(key.toString+","+temp._1), new IntWritable(temp._2)))
  }

  override def cleanup(context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
    getLogger(this.getClass).info("Job 1 Reducer Task Completed")
  }
}

