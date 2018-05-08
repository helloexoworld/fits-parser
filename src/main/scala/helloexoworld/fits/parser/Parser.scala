package helloexoworld.fits.parser

import java.io.File
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl.{FileIO, Flow, GraphDSL, RunnableGraph, Source}
import akka.stream.{ActorMaterializer, ClosedShape, FlowShape}
import akka.util.ByteString
import com.clevercloud.warp10client.Warp10Data
import helloexoworld.fits.parser.dataformat._
import helloexoworld.fits.parser.pipeline.stages.{BinTableStage, HDUParserStage, PrimaryHDUParserStage}

object Parser extends App{

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()
  val dir = Paths.get(".")

  val token = System.getenv("token")

  val fileName="result.warp10data.debug"
  val sink= FileIO.toFile(new  File(fileName))
  //  val resultSink = new Upload2Warp10Stage(Uri.uri("http://localhost:8080"), token, 5000)

  val debut = System.currentTimeMillis()

  val parsing = GraphDSL.create(){ implicit builder =>
    val pHDU = builder.add(new PrimaryHDUParserStage())
    val hdu = builder.add(new HDUParserStage())
    val bintable = builder.add(new BinTableStage(List("SAP_FLUX"), List( "OBSMODE", "INSTRUME", "OBJECT", "KEPLERID", "MISSION")))

        pHDU ~> hdu ~> bintable

    FlowShape(pHDU.in, bintable.out)
  }

  val data2warp10Data = Flow[DataPoint].map(DataPoint.toWarp10Data)
  val flowByFile = Flow[Warp10Data].map{ s => ByteString(s.warp10Serialize+"\n")}

  val globalGraph = GraphDSL.create(sink) { implicit builder => sink =>
    
      Directory.ls(dir)
        .filter(p => p.toString.endsWith(".fits"))
        .map{p=> println(p);p}

        .flatMapConcat(f => FileIO.fromPath(f, 2880).zip(Source.repeat(f)))
        .map{case (bs, p) => (p, bs)} ~> parsing ~> data2warp10Data ~>  flowByFile ~> sink

    ClosedShape
  }



  RunnableGraph.fromGraph(globalGraph)
    .run()
    .onComplete(r =>
      r.fold( error => println(s"parse error : $error"),
        result => {
          val fin = System.currentTimeMillis()
          println(s"$result points parsés en ${fin-debut} millis")
          system.terminate()
        })
  )
}
