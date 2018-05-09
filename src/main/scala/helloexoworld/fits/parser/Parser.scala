package helloexoworld.fits.parser

import java.io.File
import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl.{Balance, FileIO, Flow, GraphDSL, Merge, Source}
import akka.stream.{ActorMaterializer, FlowShape}
import akka.util.ByteString
import com.clevercloud.warp10client.Warp10Data
import helloexoworld.fits.parser.dataformat._
import helloexoworld.fits.parser.pipeline.stages.{BinTableStage, HDUParserStage, PrimaryHDUParserStage}

import scala.concurrent.ExecutionContext

object Parser extends App{

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()
  implicit val ec = ExecutionContext.global

  val dir = Paths.get("datas_sc")

  val token = System.getenv("token")

  val fileName="result.warp10data.debug"
  val sink= FileIO.toFile(new  File(fileName))

  //  val resultSink = new Upload2Warp10Stage(Uri.uri("http://localhost:8080"), token, 5000)

  val debut = System.currentTimeMillis()

  val parsing = GraphDSL.create(){ implicit builder =>
    val in = builder.add(Flow[Path].map{p=> println(p);p})
    val readFile = builder.add(Flow[Path].flatMapConcat(f => FileIO.fromPath(f, 2880).zip(Source.repeat(f)).map{case (bs, p) => (p, bs)}))
    val pHDU = builder.add(new PrimaryHDUParserStage())
    val hdu = builder.add(new HDUParserStage())
    val bintable = builder.add(new BinTableStage(List("SAP_FLUX", "PDCSAP_FLUX"), List( "OBSMODE", "INSTRUME", "OBJECT", "KEPLERID", "MISSION")))
    val data2warp10Data = builder.add( Flow[DataPoint].map(DataPoint.toWarp10Data))
    val flowByFile =  builder.add( Flow[Warp10Data].map{ s => ByteString(s.warp10Serialize+"\n")})

    in ~> readFile ~> pHDU ~> hdu ~> bintable ~> data2warp10Data ~>  flowByFile

    FlowShape(in.in, flowByFile.out)
  }

  def balancer(workerCount: Int) = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      val balancer = b.add(Balance[Path](workerCount, waitForAllDownstreams = true))
      val merge = b.add(Merge[ByteString](workerCount))

      for (i <- 1 to workerCount) {
        // for each worker, add an edge from the balancer to the worker, then wire
        // it to the merge element
        balancer ~> parsing.async ~> merge
      }

      FlowShape(balancer.in, merge.out)
    })
  }


  val globalStream = Directory.ls(dir)
        .filter(p => p.toString.endsWith(".fits"))
        .via(balancer(1))

  globalStream
    .runWith(sink)
    .onComplete(r =>
      r.fold( error => println(s"parse error : $error"),
        result => {
          val fin = System.currentTimeMillis()
          println(s"$result points parsés en ${fin-debut} millis")
          system.terminate()
        })
  )
}
