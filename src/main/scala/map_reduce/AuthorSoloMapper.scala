package map_reduce

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.LoggerFactory.getLogger

import scala.collection.convert.ImplicitConversions.`seq AsJavaList`
import scala.xml.XML


object AuthorSoloMapper extends Mapper[LongWritable, Text, Text, IntWritable]{

  private val one = new IntWritable(1)

  override def map(key: LongWritable, value: Text, context:
  Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    val dtdFileLocation = getClass.getClassLoader.getResource("dblp.dtd").toURI
    val inputXml = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
        <!DOCTYPE dblp SYSTEM "$dtdFileLocation">
        <dblp>"""+ value.toString + "</dblp>"

    val preprocessedXML = XML.loadString(inputXml)

    // extracting list of authors
    val authors = (preprocessedXML \\ "author").map(author => author.text.toLowerCase.trim).toList
    if (authors.size == 1) {
      val author = (preprocessedXML \\ "author").text
      context.write(new Text(author), one)
    }

  }

  override def cleanup(context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    getLogger(this.getClass).info("Job 6 Mapper Task Completed")
  }

}
