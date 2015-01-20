package proj2
import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.Props
import akka.actor.ActorSystem
import scala.util.Random
import scala.concurrent.duration.Duration
import scala.collection.mutable.ArrayBuffer
import akka.actor.ActorRef
import akka.actor.ActorSelection.toScala
import akka.actor.actorRef2Scala

object projectTwo {
  def main(args: Array[String]){
	  if(args.length != 3){
	    println("Incorrect input parameters.")
	  }
	  else {
	    val LOTR = ActorSystem("Gondor")
	    val numNodes=args(0).toInt
	    val topology=args(1)
	    val algorithm=args(2)
		val gandalf = LOTR.actorOf(Props(new Lord(numNodes,topology,algorithm)),"master")
	  }
	}

class Lord(numNodes:Int, topology:String, algo:String) extends Actor {

	val n = numNodes
	val hobbitContext = ActorSystem("Gondor")
	var hobbitDoneState = Array.fill[Boolean](n)(false)
	var startTime:Long = 0
    for (i<- 1 to n){
      var msg:String="init"
      val frodo = hobbitContext.actorOf(Props(new shireWorker),"HobbitNode"+ i.toString)
      frodo ! msg
    }
	topology.toLowerCase() match {
	  case "line" => 
	    for(i <- 1 to n){
		  var msg:String="l"+i.toString +","
		  val targetChild = hobbitContext.actorSelection("/user/HobbitNode"+ i)
		  if(i-1 > 0) 
			  msg = msg + (i-1).toString +","
		  if(i < n)
			  msg = msg + (i+1).toString 
		  targetChild ! msg
	    }
	    
	  case "full" =>
	    for(i <- 1 to n){
	      var msg:String="f"+n.toString
		  val targetChild = hobbitContext.actorSelection("/user/HobbitNode"+ i) 
		  msg = msg + ","+i.toString
		  targetChild ! msg
	    }
	    
	  case "2d" =>
	    val magicNumber = math.sqrt(n).floor.toInt
	    val actualN = math.pow(magicNumber, 2).toInt
	    for(i <- 1 to actualN){
	      var msg:String="2"+i.toString
		  val targetChild = hobbitContext.actorSelection("/user/HobbitNode"+ i) 
		  if(i-magicNumber > 0) msg = msg + "," + (i-magicNumber).toString  
		  if(i+magicNumber <= actualN) msg = msg + "," + (i+magicNumber).toString  
		  if(i % magicNumber == 0) msg = msg + "," + (i-1).toString 
		  else if (i % magicNumber == 1) msg = msg + "," + (i+1).toString 
		  else msg = msg + "," + (i-1).toString + "," + (i+1).toString 
		  println(targetChild+" : "+msg)
		  targetChild ! msg
	    }
	    
	    case "imperfect2d" =>
		    val magicNumber = math.sqrt(n) .floor.toInt
		    val actualN = math.pow(magicNumber, 2).toInt
		    var ringInt = Int.MaxValue
		    while (ringInt > actualN || ringInt == 0) {
			  ringInt = (Random.nextInt % actualN).abs
		    }
		    for(i <- 1 to actualN){
			  var msg:String="m"+i.toString
			  val targetChild = hobbitContext.actorSelection("/user/HobbitNode"+ i) 
			  if(i-magicNumber > 0) msg = msg + "," + (i-magicNumber).toString  
			  if(i+magicNumber <= actualN) msg = msg + "," + (i+magicNumber).toString  
			  if(i % magicNumber == 0) msg = msg + "," + (i-1).toString
			  else if (i % magicNumber == 1) msg = msg + "," + (i+1).toString 
			  else msg = msg + "," + (i-1).toString + "," + (i+1).toString 
			  msg = msg + "," + ringInt.toString   
			  println(targetChild+" : "+msg)
			  targetChild ! msg
		    }
	}
	println("--> Topology Built <--")
	algo.toLowerCase() match {
	  case "gossip"=>
	    val msg:String="g"+"Dumbledore and Gandalf are twins "
	    var ringInt = Int.MaxValue
	    while (ringInt > n || ringInt == 0) {
		  ringInt = (Random.nextInt % n).abs
	    }
	    val targetChild = hobbitContext.actorSelection("/user/HobbitNode"+ ringInt)
	    targetChild ! msg
	  case "push-sum"=>
	    val msg:String="p"+0.0.toString + "," + 0.0.toString
	    var ringInt = Int.MaxValue
	    while (ringInt > n || ringInt == 0) {
		  ringInt = (Random.nextInt % n).abs
	    }
	    val targetChild = hobbitContext.actorSelection("/user/HobbitNode"+ ringInt)
	    targetChild ! msg
	  case whatever =>
	    println("Wrong algorithm provided")
	}
	
	startTime = System.currentTimeMillis()
	
	def receive = {
		case l:String =>
		  l.toLowerCase().head match {
		    case 'd' => 
		        var revMsg = l.tail.split(",")
		        hobbitDoneState.update((revMsg(0).toInt-1), true)
		        var childCount =0;
		        for (x <- hobbitDoneState)
		          if(x == true) childCount += 1
		          println("Time taken: " + (System.currentTimeMillis() - startTime).toString+"ms")
		        if (revMsg.size <= 1){
		        var percentCovered = (childCount*100.0/n)
			        println("Percentage of nodes covered: "+percentCovered.toString)
			        if(percentCovered > 95.0) {
			          context.children.foreach(context.stop(_))
			          System.exit(0)
			          
			        }
		        }
		        else {
		        	println("Final Ratio: "+revMsg(1))
		        	context.children.foreach(context.stop(_))
			        System.exit(0)
			        
		        }
		    case 'e' => 
		        println("ERROR SHUTODWN")
		        context.children.foreach(context.stop(_))
		        println("stopped Sauron, my work is done!")
		        System.exit(1)
		    case whatever =>
		        println("Lord has received this "+whatever)
		  }
    }
}

class shireWorker extends Actor {
  import context._
  var parentNode:ActorRef = null
  var myCount =0
  var gossipKingdom = true
  var rumor =""
  var s=0.0; var w=1.0; var ratio = 0.0; var ratio_old =999.0; var dratio = 999.0;
  var elfCount = 0
  var myActiveLines = ArrayBuffer[String]()
  def transmitMsg = {
	  val msg = if(gossipKingdom == true)"rt"+rumor else "rf,"+s.toString+","+w.toString
	  val len = myActiveLines.length
	  var ringInt = Int.MaxValue
	  while (ringInt > len || ringInt == 0) {
		  ringInt = (Random.nextInt % len).abs
	  }
	  val targetBro = context.actorSelection("/user/HobbitNode"+myActiveLines(ringInt))
	  targetBro ! msg
  }
  def receive = {
    case l:String =>
      l.head match{
        case 'i' =>
          parentNode = sender
        case 'l' =>
          myActiveLines ++= l.tail.split(",")
        case 'r' =>
          l.tail.head match {
            case 't'=> gossipKingdom = true
            case 'f'=> gossipKingdom = false
          }
          if (gossipKingdom == true && myCount< 10) {
	          myCount +=1
	          rumor = l.tail.tail
	          val sendmsg = "d"+myActiveLines(0);
        	  	parentNode ! sendmsg;
	          transmitMsg
          }
          
		  if(gossipKingdom == true &&  myCount <9) {
		    val dur = Duration.create(50, scala.concurrent.duration.MILLISECONDS);
		    val me = context.self
		    context.system.scheduler.scheduleOnce(dur, me, "z")
		  }
          else if (myCount == 10) {
        	  myCount+=1;
        	  val sendmsg = "d"+myActiveLines(0);
        	  parentNode ! sendmsg;
        	  println(context.self +" is done.")
          }
          else if (gossipKingdom == false) {
        	  if (s == 0.0) {
        		  s = myActiveLines(0).toDouble
        	  }
        	  var inParams = l.tail.tail.split(",")
        	  s = (s+inParams(1).toDouble)/2
        	  w = (w+inParams(2).toDouble)/2
        	  ratio_old = ratio
        	  ratio = s/w
        	  println(self+": "+ratio.toString)
        	  dratio = (ratio-ratio_old).abs
        	  if(dratio < 0.0000000001) elfCount += 1
        	  else elfCount = 0
        	  if(elfCount == 3) {
        		  val sendmsg = "d"+myActiveLines(0)+","+ratio.toString;
        		  parentNode ! sendmsg;
        	  }
        	  else if (elfCount < 3) {
        		  transmitMsg
        	  }
        	  else if (elfCount > 3) {
        		  println("Sauron is getting messages")
        	  }
          }
          
        case 'f' =>
          val input = l.tail.split(",")
          val numNodes = input(0).toInt
          val myNumber = input(1)
          for (i <- 1 to numNodes)
            myActiveLines += i.toString
          myActiveLines.update(0, myNumber)
          myActiveLines.update((myNumber.toInt - 1), 1.toString)
        case '2' =>
          myActiveLines ++= l.tail.split(",")
        case 'm' =>
          myActiveLines ++= l.tail.split(",")
        case 'o' =>
          myActiveLines ++= l.tail.split(",")
        case 'z'=>
          transmitMsg
        case 'g'=>
          gossipKingdom = true
          rumor = l.tail
          val sendmsg = "d"+myActiveLines(0);
        	  parentNode ! sendmsg;
          transmitMsg
        case 'p'=>
          gossipKingdom = false
          s = myActiveLines(0).toDouble
          ratio = s/w
          dratio = (ratio_old-ratio).abs
          transmitMsg
        case whatever => {println("error in Mordor, got this: "+whatever); sender ! "error"}
      }
  }
}
}
