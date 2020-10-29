package map_reduce

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.LoggerFactory

import scala.collection.mutable.TreeSet

object ConsecutiveReducer extends Reducer[Text, Text, Text, IntWritable]{
  override def reduce(key: Text, values: lang.Iterable[Text],
                      context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
    val authorYearSet = new TreeSet[Int]()
    var count: Int = 0
    values.forEach(value => authorYearSet.add(value.toString.toInt))

    //checking if author has published for 10 consecutive year
    if(authorYearSet.size >= 10) {
      (0 until authorYearSet.size-1).foreach (
        i => if(authorYearSet(i) == authorYearSet(i+1)) count+=1)
      if(count >= 10)
        context.write(new Text(key.toString), new IntWritable(count))
    }
  }
  override def cleanup(context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
    LoggerFactory.getLogger(this.getClass).info("Job 2 Reducer Task Completed")
  }
}