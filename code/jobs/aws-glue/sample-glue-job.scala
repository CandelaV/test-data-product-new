import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{explode, explode_outer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}
import org.apache.spark.sql.streaming.Trigger
import scala.collection.JavaConverters._
import com.databricks.spark.xml._

object SampleParser {
    
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sparkSession: SparkSession = glueContext.getSparkSession
    import sparkSession.implicits._
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "filename", "bucketname").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val bucketName = args("bucketname")
    val fileName = args("filename")
    val filePathS3URL = "s3://"+bucketName+"/"+fileName
    
    //TODO: Change rootid literal for your xml file root id
    val ampRawDataFrame = sparkSession.read          
      .format("xml")
      .option("rowTag", "rootid")
      .load(filePathS3URL)
      

    //TODO: Create your logic in the createDataFrameFunction
    //val dataFrame = createDataFrameFunction()
    
    //TODO: Write your data frame to an S3 bucket	
//dataFrame.write.format("parquet").mode("overwrite").save(thePath)
    
    Job.commit()
  }
}