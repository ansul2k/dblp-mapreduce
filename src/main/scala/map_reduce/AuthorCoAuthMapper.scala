package map_reduce

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.LoggerFactory.getLogger

import scala.xml.{Elem, XML}


object AuthorCoAuthMapper extends Mapper[LongWritable, Text, Text, IntWritable]{

  private val one = new IntWritable(1)

  override def map(key: LongWritable, value: Text, context:
  Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    val dtdFileLocation = getClass.getClassLoader.getResource("dblp.dtd").toURI
    val inputXml = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
        <!DOCTYPE dblp SYSTEM "$dtdFileLocation">
        <dblp>"""+ value.toString + "</dblp>"

    //loading xml string
    val preprocessedXML = XML.loadString(inputXml)

    //getting author list
    val authors = getAuthorList(preprocessedXML)

    if (authors.size > 1) authors.foreach(author => context.write(new Text(author), one))

  }

  /**
   * extract lists of authors,
   *
   * @param xml xml element
   * @return the list of authors
   */

  def getAuthorList(xml:Elem): List[String] = {
    val author = (xml \\ "author").map(author => author.text.toLowerCase.trim).toList
    author
  }

  override def cleanup(context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    getLogger(this.getClass).info("Job 5 Mapper Task Completed")
  }

}
