package map_reduce

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.LoggerFactory.getLogger

import scala.xml.{Elem, XML}

object HighestAuthorPubVenueMapper extends Mapper[LongWritable, Text, Text, Text]{

  override def map(key: LongWritable, value: Text, context:
  Mapper[LongWritable, Text, Text, Text]#Context): Unit = {

    val dtdFileLocation = getClass.getClassLoader.getResource("dblp.dtd").toURI
    val inputXml = s"""<?xml version="1.0" encoding="ISO-8859-1"?>
        <!DOCTYPE dblp SYSTEM "$dtdFileLocation">
        <dblp>"""+ value.toString + "</dblp>"

    val preprocessedXML = XML.loadString(inputXml)

    // extracting list of authors
    val authors = (preprocessedXML \\ "author").map(author => author.text.toLowerCase.trim).toList

    //extracting title
    val title = (preprocessedXML \\ "title").text

    //extracting venue
    val venue = getVenue(preprocessedXML)
    if (venue != "") context.write(new Text(venue), new Text(title +",,,,"+ authors.size))

  }

  /**
   * extract venue
   *
   * @param xml xml element
   * @return the venue
   */

  def getVenue(xml:Elem): String ={
    val typeTemp = xml.head.child
    val typePub = typeTemp.head.label
    val venue: String = typePub match {
      case "article" => (xml \\ "journal").text
      case "inproceedings" => (xml \\ "booktitle").text
      case "proceedings" => (xml \\ "booktitle").text
      case "incollection" => (xml \\ "booktitle").text
      case "book" => (xml \\ "booktitle").text
      case "phdthesis" => (xml \\ "publisher").text
      case "mastersthesis" => (xml \\ "publisher").text
      case _ => ""
    }
    venue
  }

  override def cleanup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    getLogger(this.getClass).info("Job 4 Mapper Task Completed")
  }
}
