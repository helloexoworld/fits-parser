package helloexoworld.fits.parser.pipeline.stages

import java.nio.file.Path

import akka.stream.scaladsl.{FileIO, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream._
import akka.util.ByteString
import helloexoworld.fits.parser.pipeline.stages.dataformat.{DataBlockWithHeader, String2Header}

import scala.concurrent.Future

class PathToByteStringStage() extends GraphStage[FlowShape[Path, Tuple2[Path, ByteString]]] {

  val in = Inlet[Path]("PathToFileStage.in")
  val out = Outlet[Tuple2[Path, ByteString]]("PrimaryHDUParser.out")

  override def shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
  /*    var file :Source[ByteString, Future[IOResult]]

      setHandler(in, new InHandler {
        override def onPush(): Unit =
          val path = grab(in)
          file = FileIO.fromPath(path, 2880)
          val chunk = file.
          push(out, (path, grab(in)))

      }
      )*/
      setHandler(out, new OutHandler {
        override def onPull(): Unit = { pull(in) }
      })
    }
}

