import com.amazonaws.service.glue.GlueContext
import com.amazonaws.service.glue.util.GlueArgParser
import com.amazonaws.service.glue.util.Job
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row 
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.function.from_json
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.OutputMode

import scala.collection.JavaConverters._

object GlueApp{ 
    def main(sysArgs: Array[string]){
        val spark: SparkContext = new SparkContext()
        val gluecontext: GlueContext = new GlueContext(spark)
        val sparSession: SparkSession = gluecontext.getSparkSession
        
        val args = GlueArgParser.getSparkSession(sysArgs, Seq("JOB_NAME").toArray)
        job.init(args("JOB_NAME"), gluecontext, args.asJava)
        
        // read input stream
        val df = sparSession.readStream
            .format("kinesis")
            .option("streamName", "data-pipe-stream")
            .option("endpointUrl", "https://kinesis.ap-noreast1-1.amazonaws.com")
            .option("startingPosition", "TRIM_HORIZON")
            
        df.writeStream()
            .format("parquet")
            .option("path", "")
            .outputMode(OutputMode.Append())
            .option("checkpointLocation", "")
            .start()
            .awaitTermination()
            
    }

}