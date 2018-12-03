/** Download sparkts-0.4.0-jar-with-dependencies.jar JAR file before running the code */


import com.cloudera.sparkts.models._
import com.cloudera.sparkts.models.ARIMA._
import breeze.linalg._
import org.apache.spark.mllib.linalg.{Vector, Vectors}

val lines = sc.textFile("hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/datacollection.db/calaqi_value/000000_0")
val records = lines.map(_.split(","))
val date = records.map(rec=>rec(0))
val dates =date.collect()
val tuples = records.map(rec=>rec(1))
val tuples = records.map(rec=>rec(1).toDouble)
val ts = tuples.collect()
val trainingSize = (ts.length * 0.85).toInt
val trainingAmounts = new Array[Double](trainingSize)
      for(i <- 0 until trainingSize)
     {
        trainingAmounts(i) = ts(i)
      }
 
import org.apache.spark.mllib.linalg.{Vector, Vectors}

      val actual = Vectors.dense(trainingAmounts)
      val period = ts.length - trainingSize
      val model = ARIMA. fitModel(100, 0, 0,actual)
      println("best-fit model ARIMA(" + model.p + "," + model.d + "," + model.q + ") AIC=" + model.approxAIC(actual) )
      val predicted = model.forecast(actual, period)
      var totalErrorSquare = 0.0
      for (i <- (predicted.size - period) until predicted.size) {
        val errorSquare = Math.pow(predicted(i) - ts(i), 2)
        println("Predicted= "+predicted(i) + "\t should be \t"+" Actual= " + ts(i) + "\t Error Square = " + errorSquare)
        totalErrorSquare += errorSquare
      }
      println("Root Mean Square Error: " +Math.sqrt(totalErrorSquare/period))
    println("Categorization of predicted models")
var goodp = 0
val predictedCat=Array.ofDim[String](predicted.size)

var moderatep = 0

var unhealthysp = 0

var unhealthyp = 0

var good = 0
var actualCat=Array.ofDim[String](predicted.size)
var predictedCat=Array.ofDim[String]( predicted.size)
var moderate = 0

var unhealthys = 0

var unhealthy = 0

var x=0
for(i <- (predicted.size - period) until predicted.size){
if (predicted(i) < 51){ 
println(dates(i)+" "+predicted(i)+" Good") 
goodp =goodp+1
predictedCat.update(x,"Good")
}
if((predicted(i) < 101) && (predicted(i) > 50)){
println(dates(i)+" "+ predicted(i)+" Moderate") 
moderatep =moderatep+1
predictedCat.update(x, "Moderate")

}
if ((predicted(i) < 151) && (predicted(i) > 100)){
println(dates(i)+" "+ predicted(i)+" Unhealthy for sensitive group") 
unhealthysp =unhealthysp+1
predictedCat.update(x,"Unhealthy for sensitive people")

}
if ((predicted(i) > 150)){ 
println(dates(i)+" "+ predicted(i)+" Unhealthy" ) 
unhealthyp =unhealthyp+1
predictedCat.update(x, "Unhealthy")
}
x=x+1
}

var x=0
for(i <- (predicted.size - period) until predicted.size){
if (ts (i) < 51){ 
//println(i+" "+ts(i)+" Good") 
good =good+1
actualCat.update(x, "Good")
}
if((ts (i) < 101) && (ts (i) > 50)){
//println(i+" "+ ts (i)+" Moderate") 
moderate =moderate+1
actualCat.update(x,"Moderate")

}
if ((ts (i) < 151) && (ts (i) > 100)){
//println(i+" "+ ts (i)+" Unhealthy for sensitive group") 
unhealthys =unhealthys+1
actualCat.update(x,"Unhealthy for sensitive group")

}
if ((ts (i) > 150)){ 
//println(i+" "+ ts (i)+" Unhealthy" ) 
unhealthy =unhealthy+1
actualCat.update(x,"Unhealthy")
}
x=x+1
}
var errorCount=0
var errorCountGood=0
var errorCountModerate=0

for (i  <- 0 until actualCat.size){
if(predictedCat(i) != actualCat(i)){
errorCount=errorCount+1
if(predictedCat(i)== "Moderate")
{
errorCountModerate= errorCountModerate+1
}
if(predictedCat(i)== "Good")
{
errorCountGood= errorCountGood+1
}

}
}

println("Error Count percentage " +(errorCount*100/predictedCat.size))

println("Accuracy for Category Good " +( 100 - ( errorCountGood *100)/good))
println("Accuracy for Category Moderate " + (100- ( errorCountModerate *100/ moderatep)))

