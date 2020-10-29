package map_reduce

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

object Driver {

  def main(args: Array[String]): Unit = {
    //BasicConfigurator.configure()
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val conf = ConfigFactory.load()

    val configuration = new Configuration
    // Mahouts Documentation for XML parser
    configuration.set("xmlinput.start", conf.getString("START_TAGS"))
    configuration.set("xmlinput.end", conf.getString("END_TAGS"))
    configuration.set(
      "io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization")

    // Job 1: Finding top ten published authors at each venue
    val job = Job.getInstance(configuration, "AuthorVenueMapReduce")
    job.setJarByClass(this.getClass)

    // Mapper for finding top ten published authors at each venue
    job.setMapperClass(AuthorVenueMapper.getClass)
    job.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    // Reducer for finding top ten published authors at each venue
    job.setReducerClass(AuthorVenueReducer.getClass)

    // Input and Output file information
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1) + conf.getString("PATH_OUTPUT1")))

    // Waiting for the job to complete
    logger.info("Starting Job 1")
    job.waitForCompletion(true)
    logger.info("Job 1 Finished")

    // Job 2: The list of authors who published without interruption for N (N>10) years
    val job2 = Job.getInstance(configuration, "AuthorYearMapReduce")
    job2.setJarByClass(this.getClass)

    // Mapper for finding the list of authors who published without interruption for N (N>10) years
    job2.setMapperClass(ConsecutiveMapper.getClass)
    job2.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[Text])

    // Reducer for finding the list of authors who published without interruption for N (N>10) years
    job2.setReducerClass(ConsecutiveReducer.getClass)

    // Input and Output file information
    FileInputFormat.addInputPath(job2, new Path(args(0)))
    FileOutputFormat.setOutputPath(job2, new Path(args(1) + conf.getString("PATH_OUTPUT2")))

    // Waiting for the job to complete
    logger.info("Starting Job 2")
    job2.waitForCompletion(true)
    logger.info("Job 2 Finished")

    // Job 3: For each venue producing the list of publications that contains only one author
    val job3 = Job.getInstance(configuration, "AuthorYearMapReduce")
    job3.setJarByClass(this.getClass)

    // Mapper for each venue you will produce the list of publications that contains only one author
    job3.setMapperClass(OneAuthorPubVenueMapper.getClass)
    job3.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[Text])

    // Input and Output file information
    FileInputFormat.addInputPath(job3, new Path(args(0)))
    FileOutputFormat.setOutputPath(job3, new Path(args(1) + conf.getString("PATH_OUTPUT3")))

    // Waiting for the job to complete
    logger.info("Starting Job 3")
    job3.waitForCompletion(true)
    logger.info("Job 3 Finished")

    // Job 4: The list of publications for each venue that contain the highest number of
    //        authors for each of these venues
    val job4 = Job.getInstance(configuration, "AuthorYearMapReduce")
    job4.setJarByClass(this.getClass)

    // Mapper for finding The list of publications for each venue that contain the highest
    // number of authors for each of these venues
    job4.setMapperClass(HighestAuthorPubVenueMapper.getClass)
    job4.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job4.setOutputKeyClass(classOf[Text])
    job4.setOutputValueClass(classOf[Text])

    // Reducer for finding The list of publications for each venue that contain the highest
    // number of authors for each of these venues
    job4.setReducerClass(HighestAuthorPubVenueReducer.getClass)

    // Input and Output file information
    FileInputFormat.addInputPath(job4, new Path(args(0)))
    FileOutputFormat.setOutputPath(job4, new Path(args(1) + conf.getString("PATH_OUTPUT4")))

    // Waiting for the job to complete
    logger.info("Starting Job 4")
    job4.waitForCompletion(true)
    logger.info("Job 4 Finished")

    // Job 5: Top 100 authors in the descending order who publish with most co-authors
    val job5 = Job.getInstance(configuration, "AuthorYearMapReduce")
    job5.setJarByClass(this.getClass)

    // Mapper for finding top 100 authors in the descending order who publish with most co-authors
    job5.setMapperClass(AuthorCoAuthMapper.getClass)
    job5.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job5.setOutputKeyClass(classOf[Text])
    job5.setOutputValueClass(classOf[IntWritable])

    // Reducer for finding top 100 authors in the descending order who publish with most co-authors
    job5.setReducerClass(AuthorCoAuthReducer.getClass)

    // Input and Output file information
    FileInputFormat.addInputPath(job5, new Path(args(0)))
    FileOutputFormat.setOutputPath(job5, new Path(args(1) + conf.getString("PATH_OUTPUT5")))

    // Waiting for the job to complete
    logger.info("Starting Job 5")
    job5.waitForCompletion(true)
    logger.info("Job 5 Finished")

    // Job 6: List of 100 authors who publish without any co-authors
    val job6 = Job.getInstance(configuration, "AuthorYearMapReduce")
    job6.setJarByClass(this.getClass)

    // Mapper for the list of 100 authors who publish without any co-authors
    job6.setMapperClass(AuthorSoloMapper.getClass)
    job6.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job6.setOutputKeyClass(classOf[Text])
    job6.setOutputValueClass(classOf[IntWritable])

    // Reducer for the list of 100 authors who publish without any co-authors
    job6.setReducerClass(AuthorSoloReducer.getClass)

    // Input and Output file information
    FileInputFormat.addInputPath(job6, new Path(args(0)))
    FileOutputFormat.setOutputPath(job6, new Path(args(1) + conf.getString("PATH_OUTPUT6")))

    // Waiting for the job to complete
    logger.info("Starting Job 6")
    job6.waitForCompletion(true)
    logger.info("Job 6 Finished")
  }
}
