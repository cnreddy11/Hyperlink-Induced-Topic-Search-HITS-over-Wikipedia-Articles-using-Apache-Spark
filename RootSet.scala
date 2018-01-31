import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

class RootSet(sc: SparkContext) {

	def rootSet(t: String, word: String) : RDD[(String, Long)] = {
        val titles = sc.textFile(t)
        val rootSet = titles.zipWithIndex().filter(t => t._1.toLowerCase.split("_").contains(word.toLowerCase))  //replaced matches with equals               
  	    val newRootSet = rootSet.map(t => (t._1,t._2 + 1))
  		return newRootSet
   	}
   	
   	// method to convert list of lists to a list
   	def flatten(ls: List[Any]): List[Any] = ls flatMap {
   		case ms: List[_] => flatten(ms)
   		case e => List(e)
  	} 
   	
   	def baseSet(rm: RDD[(Int,String)], l: String, t: String) : RDD[(String, List[java.io.Serializable])] = {
   	
   	    val linksFile = sc.textFile(l)
        val root = rm.collectAsMap.keySet
        val titles = sc.textFile(t)
        val baseSetNums = scala.collection.mutable.HashMap.empty[String, List[java.io.Serializable]]
        
        val links = linksFile.filter(t => {
                        val l = t.split(": ")
                        root.contains(l(0).toInt) ||
                        l(1).split(" ").exists { x =>
                            root.contains(x.toInt)
                        }
                    })
            
        val link = links.collect.toList
             		
        for(l <- link){
        	val s = l.split(": ")
        	
        	//from root
        	if(root.contains(s(0).toInt))
        	{
        		val linksFromRoot = s(1).split(" ")
        		val buf = scala.collection.mutable.ListBuffer.empty[String]
        		for(li <- linksFromRoot)
        			buf += li
        		buf.toList
        		if(!baseSetNums.contains(s(0)))
        			baseSetNums(s(0)) = buf.toList
        		else
        		{
        			val x = baseSetNums.get(s(0))
        			val d = x ++ buf
        			baseSetNums(s(0)) = d.toList
        		} 
        	}
        	
        	//to root
        	else{
        	   	val t = s(1).split(" ")
        		for(nums <- t){
        			if(root.contains(nums.toInt)){
        				val buf = scala.collection.mutable.ListBuffer.empty[String]
        				buf += nums
        				buf.toList
        				if(!baseSetNums.contains(s(0))){
        					baseSetNums(s(0)) = buf.toList 
        				}
        				else{
        					val x = baseSetNums.get(s(0))
        					val d = List.concat(x,buf)
        					baseSetNums(s(0)) = d.toList
        				}
        			}
				}        	
        	}
        }
        
        
 
        for(bs <- baseSetNums.keySet){
        	val d = baseSetNums.get(bs)
        	baseSetNums(bs) = flatten(d.toList).map(_.toString).distinct
        }
        val baseSet = sc.parallelize(baseSetNums.toList)
        
        return baseSet
   	}
   	
   	
   	def diffCalc(newVal : RDD[(Int, Double)], oldVal : RDD[(Int, Double)]) : Boolean = {
   	
   		val diff = newVal.join(oldVal).map{case(a,(b,c)) => (a,b-c)}.filter(t => t._2 < 0.00000001) //precision of 10^-12
		if( diff.count() < 8)
			return true
		else
			return false    	
   	}
   	
   	def iterations(bs : RDD[(String, List[java.io.Serializable])], rs : RDD[(Int, String)], output : String) 
   						: RDD[(String, Double)] = {
   		
   		
   		var x = bs.flatMapValues(f => ((f mkString ",").split(","))).distinct
   		var nodes = x.keys.distinct.union(x.values.distinct)
   		
   		var hub = nodes.map(t => (t.toInt, 1.0))
   		var auth = nodes.map(t => (t.toInt, 1.0))
   		
   		//Converting baseSet to (From,To) map
   		
   		//val fromToD = bs.flatMapValues(f => ((f mkString ",").split(",")))
   		val fromTo = x.map{case(a,b) => (a.toInt,b.toInt)}
   		val toFrom = fromTo.map(t => (t._2,t._1))
   		
   		
   		//iterations to calculate Hub and authority scores
   		var i = 0
   		var merged = false   		
   		do{
   			
   			val authJoin = toFrom.join(hub).map{case(a,(b,c)) => (a,c)}.reduceByKey((a,b) => a+b)
   			val hubJoin = fromTo.join(auth).map{case(a,(b,c)) => (a,c)}.reduceByKey((a,b) => a+b)   			
   			val authTotal = authJoin.values.sum
   			val hubTotal = hubJoin.values.sum
   			val authNormalized = authJoin.map( t => (t._1, t._2/(authTotal)))
   			val hubNormalized = authJoin.map( t => (t._1, t._2/(hubTotal)))
   			
   			merged = diffCalc(authNormalized, auth) && diffCalc(hubNormalized,hub) 
   			i = i+1
   			auth = authNormalized
   			hub = hubNormalized
   			
   		}while(i <= 34 && !merged )
   		
   		
   		val authSorted = sc.parallelize(auth.coalesce(1,true).sortBy(_._2,false).take(50))
   		val hubSorted = sc.parallelize(hub.coalesce(1,true).sortBy(_._2,false).take(50))
   		val authFinal = rs.join(authSorted).map{case(a,(b,c)) => (b,c)}
   		val hubFinal = rs.join(hubSorted).map{case(a,(b,c)) => (b,c)}
   		
   		authFinal.coalesce(1,true).saveAsTextFile(output+"/Authority")
   		hubFinal.coalesce(1,true).saveAsTextFile(output+"/Hub")
   		
   		return authFinal
   	}
}

object RootSet {
  def main(args: Array[String]) {
        val titlesFile = args(0)
        val linksFile = args(1)
        val word = args(2)
        val output = args(3)
        val conf = new SparkConf().setAppName("Root Set").setMaster("yarn")
        
        // for rootSet
        val context = new SparkContext(conf)
        val job = new RootSet(context)
        val rootSet = job.rootSet(titlesFile, word)
        rootSet.saveAsTextFile(output+"/rootSet")
        
        // for baseSet
        val rootMap = rootSet.map(t => ((t._2).toInt,t._1))         
        val baseSet = job.baseSet(rootMap, linksFile, titlesFile)
        baseSet.saveAsTextFile(output+"/baseSet")
        
        
        // for creating Iterations
        val iter = job.iterations(baseSet,rootMap,output)        
        
        context.stop()
  }
}