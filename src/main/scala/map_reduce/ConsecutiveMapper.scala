package map_reduce

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.LoggerFactory.getLogger

import scala.xml.{Elem, XML}

object ConsecutiveMapper extends Mapper[LongWritable, Text, Text, Text]{

  override def map(key: LongWritable, value: Text, context:
  Mapper[LongWritable, Text, Text, Text]#Context): Unit = {

    val dtdFileLocation = getClass.getClassLoader.getResource("dblp.dtd").toURI
    val inputXml = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
        <!DOCTYPE dblp SYSTEM "$dtdFileLocation">
        <dblp>"""+ value.toString + "</dblp>"

    val preprocessedXML = XML.loadString(inputXml)

    //extracting list of authors
    val authors = (preprocessedXML \\ "author").map(author => author.text.toLowerCase.trim).toList

    //extracting author
    val year = getYear(preprocessedXML)
    authors.foreach(author => context.write(new Text(author), new Text(year)))
  }

  /**
   * extract year
   *
   * @param xml xml element
   * @return year
   */

  def getYear(xml:Elem): String ={
    val year = (xml \\ "year").text
    year
  }

  override def cleanup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    getLogger(this.getClass).info("Job 2 Mapper Task Completed")
  }
}
