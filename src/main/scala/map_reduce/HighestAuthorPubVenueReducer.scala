package map_reduce

import java.lang

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.LoggerFactory.getLogger

import scala.collection.mutable.HashMap

object HighestAuthorPubVenueReducer extends Reducer[Text, Text, Text, Text]{
  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val PubAuthorHMap = new HashMap[String, Int]()
    values.forEach { value =>
      val temp = value.toString.split(",,,,")
      PubAuthorHMap.put(temp(0),temp(1).toInt)
    }
    //calculating maximum value of publication at a venue
    val res = PubAuthorHMap.maxBy(item => item._2)
    val pubAuthorHMapFilter = PubAuthorHMap.filter(item => item._2 == res._2)
    pubAuthorHMapFilter foreach ( pubAuthor => context.write(key, new Text(pubAuthor._1)))
  }

  override def cleanup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    getLogger(this.getClass).info("Job 4 Reducer Task Completed")
  }
}
