package helloexoworld.fits.parser

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.dispatch.Filter
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.util.ByteString
import helloexoworld.fits.parser.pipeline.stages.dataformat.{DataBlockWithHeader, DataPoint, DataTable}
import helloexoworld.fits.parser.pipeline.stages.{BinTableStage, HDUParserStage, PrimaryHDUParserStage, Upload2Warp10Stage}
import org.http4s.Uri

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Parser extends App{

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()
  val file = Paths.get("ktwo200001049-c01_llc.fits")
  //val file = Paths.get("kplr002013502-2009131105131_llc.fits")

  val token = "GjtzhY2Ns6zNejXbwE93MEmbl8JBEZI8H5utnlsqDIsI.tsu8IRthDpUWnKCcXHXNYQs4reE47.FzdJ0I2ruaMXynHJzqD19R67ZD.jIMCB7E9Xam79KFUtJo0YIdTSQ"

  val resultSink = new Upload2Warp10Stage(Uri.uri("http://localhost:8080"), token, 200)

  val g = RunnableGraph.fromGraph(GraphDSL.create(resultSink) { implicit builder => sink =>

    val pHDU = builder.add(new PrimaryHDUParserStage())
    val hdu = builder.add(new HDUParserStage())
    val bintable = builder.add(new BinTableStage(List("SAP_FLUX", "PSF_CENTR2"), "KEPLERID"))

   // val sink = builder.add(new Upload2Warp10Stage(Uri.uri("http://localhost:8080"), token, 200))

    val printer = builder.add(Flow[DataPoint].map { s => println(s); s })

    FileIO.fromPath(file, 2880) ~> pHDU.in;
    pHDU.out ~> hdu.in;
    hdu.out ~> bintable.in;
    bintable.out ~> printer.in;
     bintable.out ~> printer.in;
    printer.out ~> sink.in;
    ClosedShape
  })

 /* val mat = FileIO.fromPath(file, 2880)
    .via(new PrimaryHDUParserStage())
    .via(new HDUParserStage())
    .via(new BinTableStage(List("SAP_FLUX", "PSF_CENTR2"), "KEPLERID"))
    .toMat(resultSink)(Keep.right)
      .run()
    .onComplete(system.terminate())
*/
  val debut = System.currentTimeMillis()

  g.run().onComplete{r =>

    r.fold( error => println(s"parse error : $error"),
      result => {
        val fin = System.currentTimeMillis()
        println(s"${result} points parsés en ${fin-debut} millis")
      })
    system.terminate()
  }
}
