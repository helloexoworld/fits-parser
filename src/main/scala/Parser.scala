package helloexoworld.fits.parser

import java.io.File
import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import akka.util.ByteString
import com.clevercloud.warp10client.{DoubleWarp10Value, IntWarp10Value, LongWarp10Value, Warp10Data}
import helloexoworld.fits.parser.pipeline.stages._
import helloexoworld.fits.parser.pipeline.stages.dataformat.{ByteValue, DataPoint, DataValue, DoubleValue, FloatValue, IntValue, LongValue}
import org.http4s.Uri

import scala.concurrent.ExecutionContext.Implicits.global

object Parser extends App{

  implicit val system = ActorSystem("QuickStart")
  implicit val materializer = ActorMaterializer()
  val dir = Paths.get("datas_lc")
  //Paths.get("/Users/emmanuelfeller/dev/git/github/helloexoworld/kepler-lens/kepler-data/lightcurves/public_Q0_long_1")
 // val file = Paths.get("ktwo200001049-c01_llc.fits")
  //val file = Paths.get("kplr002013502-2009131105131_llc.fits")

  val token = "1rdyNKjvgDmMPI2WI6Fv1OZrHJn6vPX89HREFXqJlA52wzjNq5gpa6zSTBF86PfPq9EfiQ0nWXphT327MDmmPzQ0xBruUvPeNB282KpwW4hqCZANd_2VyrbNdUHr1PPj"

  val resultSink = new Upload2Warp10Stage(Uri.uri("http://localhost:8080"), token, 5000)

  val parsing = GraphDSL.create(){ implicit builder =>
    //val flowPath = builder.add(Flow.fromSinkAndSource())
    val pHDU = builder.add(new PrimaryHDUParserStage())
    val hdu = builder.add(new HDUParserStage())
    val bintable = builder.add(new BinTableStage(List("SAP_FLUX", "PDCSAP_FLUX"), List("INSTRUME", "OBJECT", "KEPLERID", "MISSION")))
/*
INSTRUME= ‘Kepler Photometer’ / detector type
OBJECT = ‘KIC 11853905’ / string version of target id
KEPLERID= 11853905 / unique Kepler target identifier
MISSION = ‘Kepler ’ / Mission name
 */
    //flowPath ~> pHDU;
    pHDU ~> hdu;
    hdu ~> bintable;
    FlowShape(pHDU.in, bintable.out)
  }.named("parser")
val fileName="result.warp10data.lc"
 val f =new  File(fileName)
  val sink= FileIO.toFile(f)

  val g = RunnableGraph.fromGraph(GraphDSL.create(sink) { implicit builder => sink =>



      val balancedParsing = builder.add(Parallelism.balancer(parsing, 1))
    val data2warp10Data = Flow[DataPoint].map{dataPoint =>
      def warp10Name(name:String)= name.replace('_', '.').toLowerCase()

      def warp10data(value: DataValue) = value match{
        case data : FloatValue  => DoubleWarp10Value(data.value)
        case data : DoubleValue => DoubleWarp10Value(data.value)
        case data : IntValue    => IntWarp10Value(data.value)
        case data : ByteValue   => IntWarp10Value(data.value)
        case data : LongValue   => LongWarp10Value(data.value)
      }

      val time = dataPoint.time2timestamp * 1000
      Warp10Data(time, None, warp10Name(dataPoint.name), dataPoint.headers.toSet, warp10data(dataPoint.value))
    }

    val flowByFile = Flow[Warp10Data].map{ s => ByteString(s.warp10Serialize+"\n")}

      Directory.ls(dir)
       // .map{p=> println(p);p}

        .flatMapConcat(f => FileIO.fromPath(f, 2880).zip(Source.repeat(f)))
        .map{case (bs, p) => (p, bs)} ~> balancedParsing ~> data2warp10Data ~>  flowByFile~>sink;

    // ~> printer.in;
    //printer.out
    ClosedShape
  })

  val debut = System.currentTimeMillis()

  g.run().onComplete(r =>

    r.fold( error => println(s"parse error : $error"),
      result => {
        val fin = System.currentTimeMillis()
        println(s"${result} points parsés en ${fin-debut} millis")
        system.terminate()
      })
  )
}
