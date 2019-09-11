// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program. 

import java.io._
import Numeric.Implicits._

val inputFilePath  = "./sample_input.txt"
val outputDirPath = "./output.txt"

// Write your solution here

def BConversion (input : String) : Long = {
	if (input.endsWith("KB")) {
		val bytes = input.slice(0, input.indexOf("K")).toLong * 1024
		return bytes
	}
	else if (input.endsWith("MB")) {
		val bytes = input.slice(0, input.indexOf("M")).toLong * 1024 * 1024
		return bytes
	}
	else {
		val bytes = input.slice(0, input.indexOf("B")).toLong
		return bytes
	}
}
	
def getVariance [Arr : Numeric](it : Iterable[Arr]) : Long = {
	val avg = it.sum.toLong / it.size
	val variance = it.map(_.toLong).map(data => math.pow(data - avg, 2)).sum.toLong / it.size
	return variance
}

def getValues(data : (String, Iterable[Long])) : String = {
	val min = data._2.min.toString + "B"
	val max = data._2.max.toString + "B"
	val mean = (data._2.sum / data._2.size).toString + "B"
	val variance = getVariance(data._2).toString + "B"
	val a = data._1 + " " + min + ", " + max + ", " + mean + ", " + variance + "\n"
	//println(a)
	return a
}

val file = sc.textFile(inputFilePath, 1)
val raw_data = file.map(row => row.split(","))
//raw_data.foreach(println)
val before_reduce = raw_data.map(mappedElements => (mappedElements(0).toString, BConversion(mappedElements(3))))
//before_reduce.foreach(println)
val after_reduce = before_reduce.groupByKey
val output = after_reduce.map(data => getValues(data))
output.saveAsTextFile(outputDirPath)
