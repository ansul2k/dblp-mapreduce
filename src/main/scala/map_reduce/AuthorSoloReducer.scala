package map_reduce

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.LoggerFactory.getLogger

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`map AsJavaMap`
import scala.collection.immutable.{ListMap, TreeMap}

object AuthorSoloReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

  var venueCountTMap: TreeMap[String, Integer] = _

  override def setup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    venueCountTMap = new TreeMap[String, Integer]()
  }

  override def reduce(key: Text, values: lang.Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    //summing up the values
    val sum = values.asScala.foldLeft(0)(_ + _.get)
    venueCountTMap.put(key.toString, sum)
  }

  override def cleanup(context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    //sorting and taking top 100 values
    val venueCountTMapSorted = ListMap(venueCountTMap.toSeq.sortWith(_._2 > _._2):_*).take(100)
    venueCountTMapSorted.entrySet.forEach( entry =>
      context.write(new Text(entry.getKey), new IntWritable(entry.getValue))
    )
    getLogger(this.getClass).info("Job 6 Reducer Task Completed")
  }
}

