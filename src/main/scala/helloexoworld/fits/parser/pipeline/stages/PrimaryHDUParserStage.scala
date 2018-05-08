package helloexoworld.fits.parser.pipeline.stages

import java.nio.file.Path

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import helloexoworld.fits.parser.dataformat._

class PrimaryHDUParserStage() extends GraphStage[FlowShape[Tuple2[Path, ByteString], DataBlockWithHeader]] {

  val in = Inlet[Tuple2[Path, ByteString]]("PrimaryHDUParser.in")
  val out = Outlet[DataBlockWithHeader]("PrimaryHDUParser.out")

  override def shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging{
      var headers=Map[String, String]()
      var isInHeaders = true
      var fileName=""
      var naxisDimSize = List.empty
      var isNewFile = true
      var toggleNewFile=true

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          log.debug("onPush PrimaryHDU")
          val (path, block) = grab(in)
          isNewFile=path.toString != fileName
          if (isNewFile) {
            toggleNewFile = true
            isInHeaders = true
            headers = Map[String, String]()
            naxisDimSize = List.empty
            fileName = path.toString
          }
          if (isInHeaders) {
       //     println("########################################    Primary HDU  block      #####")
            val lines = block.utf8String
              .grouped(80).toList
              .map { s => s.header }
              .filterNot { case (key, _) => key.isEmpty }

            headers = headers ++ lines.toMap
          //  lines.foreach(println)
            if (lines.toMap.keySet.contains("END")) {
              isInHeaders = false
            }
            pull(in)
          } else {
            push(out, (toggleNewFile, headers, block))
            toggleNewFile =false
          }
        }
      }
      )
      setHandler(out, new OutHandler {
        override def onPull(): Unit = { pull(in) }
      })
    }
}

