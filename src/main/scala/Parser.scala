import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.util.ByteString

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Parser extends App{

type DataBlockWithHeader=Tuple2[Map[String, String],ByteString]
class HDUParser() extends GraphStage[FlowShape[DataBlockWithHeader, DataBlockWithHeader]] {

  val in = Inlet[DataBlockWithHeader]("HDUParser.in")
  val out = Outlet[DataBlockWithHeader]("HDUParser.out")

  override def shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var primaryHeaders=Map[String, String]()
      var headers=Map[String, String]()
      var isInHeaders = true
      var naxisDimSize = List.empty
      var dataBlockRemining = 0

      setHandler(in, new InHandler {
        override def onPush(): Unit =
          if(isInHeaders) {
            val (entete, block) = grab(in)
            primaryHeaders = entete
            println("########################################       block      #####")
            val lines = block.utf8String
              .grouped(80)
              .toList
              .map{s =>
                val key = s.take(8)
                val value = s.substring(10) match {
                  case s:String if s.startsWith("\'") =>
                    val posQuote = s.indexOf('\'', 1)
                    s.substring(0, posQuote+1)
                  case s:String if s.indexOf ('/') < 11 => ""
                  case s : String => s.substring (10, s.indexOf ('/') )

                }
                key.trim -> value.trim
              }

            headers=headers++lines.toMap
            lines.foreach(println)
            if(lines.toMap.keySet.contains("END")){
              isInHeaders=false
              val dataLength = (
                headers.get("NAXIS1").map(s => s.toInt).getOrElse(0)
                  *  headers.get("NAXIS2").map(s => s.toInt).getOrElse(0))
              val blocIncomplet = if(dataLength%2880 > 0) {1}else{0}
              dataBlockRemining = dataLength / 2880 + blocIncomplet
              println(s"Fin des headers => waiting $dataBlockRemining data blocks ")
            }
            pull(in)
          }else{
            dataBlockRemining= dataBlockRemining-1
            if(dataBlockRemining==0)isInHeaders=true
            push(out, (primaryHeaders++headers, grab(in)._2))
          }
      }
      )
      setHandler(out, new OutHandler {
        override def onPull(): Unit = { pull(in) }
      })
    }
}
class PrimaryHDUParser() extends GraphStage[FlowShape[ByteString, DataBlockWithHeader]] {

  val in = Inlet[ByteString]("PrimaryHDUParser.in")
  val out = Outlet[DataBlockWithHeader]("PrimaryHDUParser.out")

  override def shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var headers=Map[String, String]()
      var isInHeaders = true
      var naxisDimSize = List.empty

      setHandler(in, new InHandler {
        override def onPush(): Unit =
          if(isInHeaders) {
            val block:ByteString = grab(in)
            println("########################################       primary block      #####")
            val lines = block.utf8String
              .grouped(80)
              .toList
              .map{s =>
                val key = s.take(8)
                val value = s.substring(10) match {
                  case s:String if s.startsWith("\'") =>
                    val posQuote = s.indexOf('\'', 1)
                    s.substring(0, posQuote+1)
                  case s:String if s.indexOf ('/') < 11 => ""
                  case s : String => s.substring (10, s.indexOf ('/') )

                }
                key -> value
              }

            headers=headers++lines.toMap
            lines.foreach(println)
            if(lines.toMap.keySet.contains("END     ")){
              isInHeaders=false
              println("Fin du primary header")
            }
            pull(in)
          }else{
            push(out, (headers, grab(in)))
          }
      }
      )
      setHandler(out, new OutHandler {
        override def onPull(): Unit = { pull(in) }
      })
    }
}

  import akka.stream.scaladsl._
  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()
  val file = Paths.get("kplr002013502-2009131105131_llc.fits")

  val resultSink = Sink.seq[ByteString]

  val g = RunnableGraph.fromGraph(GraphDSL.create(resultSink) { implicit builder => sink =>

    val B = builder.add(new PrimaryHDUParser())
    val C = builder.add(new HDUParser())
    val D = builder.add(Flow[DataBlockWithHeader].map { s => println(s._2); s._2 })
    val debut = System.currentTimeMillis()

    FileIO.fromPath(file, 2880) ~> B.in; B.out ~> C.in; C.out ~> D.in;
    D.out ~> sink.in;
    val fin = System.currentTimeMillis()
    println(s"Duration : ${(fin-debut)}")
    ClosedShape
  })
  val r = g.run().onComplete(_ => system.terminate())
    // .onComplete()
 // Await.result(r, 1.seconds)
}
