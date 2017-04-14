import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.bdgenomics.adam.rdd.ADAMContext._
import collection.JavaConverters._
import scala.io.Source
import scala.util.Random
import java.util.Calendar
 
object clustVariants {
    def main(args: Array[String]) 
    {
        val panelfile = args(0)
        val adamfile = args(1)
        val conf = new SparkConf().setAppName("clustVariant")
        val sc = new SparkContext(conf)
        
        val biggroups = sc.parallelize(Source.fromFile(panelfile).getLines.drop(1).map(_.split("\t")).map(x => x.slice(1,2)(0)).toList).map(x => (x,1)).reduceByKey(_ + _).filter(x => x._2 > 90).map(_._1).collect
        val people = Source.fromFile(panelfile).getLines.drop(1).map(_.split("\t")).filter(x => biggroups.toList.contains(x.slice(1,2)(0))).toSet

        println("Populations with more than 90 individuals: "+biggroups.size) 
        println("Individuals from these populations: "+people.size)


        val data = sc.loadGenotypes(adamfile).rdd
        def convertAlleles(x: java.util.List[org.bdgenomics.formats.avro.GenotypeAllele] ) = { x.asScala.map(_.toString).count(_!="Ref").toFloat }

        val ndata = data.map(r => (r.contigName,r.start, r.end, r.sampleId, convertAlleles( r.alleles))).cache()

        val totalVariants = ndata.map(x => (x._1,x._2,x._3) -> 1).reduceByKey(_ + _).cache()

        val totalVariantsSize = totalVariants.count 
        println("Total variants: " + totalVariantsSize) 

        val totalPeople = ndata.map(x => x._4).distinct.count 
        val goodSamples = totalVariants.filter( x => x._2 == totalPeople).cache()

        totalVariants.unpersist()

        val goodSampleSize = goodSamples.count.toInt 
        println("Variants with right number of samples: " + goodSampleSize)

        implicit def bool2int(b:Boolean) = if (b) 1.0 else 0.0

        val dummyGoodSample = goodSamples.map(_._1).zipWithIndex.collect.toMap

        goodSamples.unpersist()

        val simplerData = ndata.map(x => x._4 -> (dummyGoodSample.getOrElse[Long]((x._1,x._2,x._3),0).toInt,x._5)).cache()

        ndata.unpersist()

        def mapDataToVector(p: (String, Iterable[(Int, Float)])) = { val array:Array[Float] = Array.fill(goodSampleSize)(0); p._2.foreach(x => if (x._1 != 0) 
{ array(x._1-1) = array(x._1-1) + x._2 }); (p._1,array) }

        val goodPeopleData = people.map(y => y(0))

        val dataFrame = simplerData.groupByKey.map(x => mapDataToVector(x)).filter(x => goodPeopleData.contains(x._1)).cache()

        simplerData.unpersist()

        val K = 21

        val maxIter = 40

        var centroids = dataFrame.takeSample(false,K,System.nanoTime.toInt).map(_._2)

        def norm(p: Array[Float]) = { p.map( x => x * x).sum }

        def squaredDist(p: Array[Float], center: Array[Float]): Float = { norm(p.zip(center).map(x => x._1 - x._2)) }

        def addArray(p: Array[Float], v: Array[Float]) : Array[Float] = { p.zip(v).map(x => x._1 + x._2) }

        def divideAllElements(p: Array[Float], v:Float): Array[Float] = { p.map( x => x/v) }

        def closestPoint(p: Array[Float], centers: org.apache.spark.broadcast.Broadcast[Array[Array[Float]]]): (Int, Array[Float]) = { (centers.value.map( x => squaredDist(p,x)).zipWithIndex.par.minBy(_._1)._2,p) }

        def closestDistance(p: Array[Float], centers: Array[Array[Float]]): Float = { centers.map( x => squaredDist(p,x)).min }
        
        var iter = 0

        import scala.util.control.Breaks._

        var centers = sc.broadcast(centroids)
        var closest = dataFrame.map( x => (x._1,closestPoint(x._2, centers)))
        centers.unpersist()
        val initArr: (Array[Float],Int) = (Array.fill(goodSampleSize)(0),0)
        val addArr = (s: (Array[Float],Int), v: (Array[Float],Int)) => { (s._1.zip(v._1).map(x => x._1 + x._2),s._2 +v._2) }
        val mergeArr = (s: (Array[Float],Int), v: (Array[Float],Int)) => { (s._1.zip(v._1).map(x => x._1 + x._2),s._2 +v._2) }

        var newCentroids = closest.map( x=> x._2._1 -> (x._2._2,1)).aggregateByKey(initArr)(addArr,mergeArr).map(x =>  divideAllElements(x._2._1,x._2._2.toFloat)).collect

        
        println("Clusters:")
        breakable { while (true) {
            iter = iter + 1
            if ( newCentroids.map( x => closestDistance(x,centroids)).sum < 1E-6 || iter == maxIter ){
                 break
             }
            centroids = newCentroids
            centers = sc.broadcast(centroids)
            closest = dataFrame.map( x => (x._1,closestPoint(x._2, centers)))
            centers.unpersist()
            newCentroids = closest.map( x=> x._2._1 -> (x._2._2,1)).aggregateByKey(initArr)(addArr,mergeArr).map(x =>  divideAllElements(x._2._1,x._2._2.toFloat)).collect
        }
        }
        println(iter)
        dataFrame.unpersist()
        val pointGroups = closest.map( x=> x._2._1 -> x._1).reduceByKey((x,y) => x ++ " " ++ y)
        pointGroups.map(_._2).collect.foreach(println)
        System.exit(0)
    }
