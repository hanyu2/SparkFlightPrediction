import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel

val flightData = spark.sqlContext.read.format("com.databricks.spark.csv").option("header","true").load("2008.csv")
flightData.registerTempTable("flights");
val airportData = spark.sqlContext.read.format("com.databricks.spark.csv").option("header","true").load("airports-csv.csv")
airportData.registerTempTable("airports");
val queryFlightNumResult = spark.sqlContext.sql("SELECT COUNT(FlightNum) FROM flights WHERE DepTime BETWEEN 0 AND 600");
queryFlightNumResult.take(1)

