package map_reduce

import com.typesafe.config.ConfigFactory
import junit.framework.TestCase
import scala.xml.XML

class mapReduceTest extends TestCase{

  val mapReduceConfig = ConfigFactory.load("application.conf")

  //test to check if config is read properly
  def testCheckConfigFile() {
    assert(!mapReduceConfig.isEmpty,"Required configurations are present.")
  }

  //test to check the value of config
  def testReadConfig() {
    val start_tags = mapReduceConfig.getString("START_TAGS").split(",")
    val end_tags = mapReduceConfig.getString("END_TAGS").split(",")
    val output1 = mapReduceConfig.getString("PATH_OUTPUT1")
    val output2 = mapReduceConfig.getString("PATH_OUTPUT2")
    val output3 = mapReduceConfig.getString("PATH_OUTPUT3")
    val output4 = mapReduceConfig.getString("PATH_OUTPUT4")
    val output5 = mapReduceConfig.getString("PATH_OUTPUT5")
    val output6 = mapReduceConfig.getString("PATH_OUTPUT6")

    assert(start_tags.length == end_tags.length && !output1.equals("") && !output2.equals("") && !output3.equals("")
      && !output4.equals("") && !output5.equals("") && !output6.equals(""))
  }

  val TRIAL_XML =s"""<?xml version="1.0" encoding="ISO-8859-1"?><dblp><article mdate="2005-08-04" key="journals/kbs/WangL97">
  <author>Huaiqing Wang</author>
  <author>Lejian Liao</author>
  <title>A framework of constraint-based modeling for cooperative decision systems.</title>
  <pages>111-120</pages>
  <year>1997</year>
  <volume>10</volume>
  <journal>Knowl.-Based Syst.</journal>
  <number>2</number>
  <ee>http://dx.doi.org/10.1016/S0950-7051(97)00007-5</ee>
  <url>db/journals/kbs/kbs10.html#WangL97</url>
  </article></dblp>"""

  val xml = XML.loadString(TRIAL_XML)

  //test to check if the venue is correct
  def testVenue {

    val venue = AuthorVenueMapper.getVenue(xml)
    assert(venue.equals("Knowl.-Based Syst."))
  }

  //test to check if the year is correct
    def testYear {

      val year = ConsecutiveMapper.getYear(xml)
      assert(year.equals("1997"))
    }

    //test to check if the title is correct
    def testTitle {

      val title = OneAuthorPubVenueMapper.getTitle(xml)
      assert(title.equals("A framework of constraint-based modeling for cooperative decision systems."))
    }

    //test to check if authors are extracted properly
    def testAuthor: Unit = {

      val authors = AuthorCoAuthMapper.getAuthorList(xml)
      assert(authors.size == 2)
    }


}
