package helloexoworld.fits.parser.pipeline.stages

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import helloexoworld.fits.parser.pipeline.stages.dataformat.DataBlockWithHeader
import helloexoworld.fits.parser.pipeline.stages.dataformat.String2Header

class PrimaryHDUParserStage() extends GraphStage[FlowShape[ByteString, DataBlockWithHeader]] {

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
            val lines = block.utf8String
              .grouped(80).toList
              .map{s => s.header}
              .filterNot{case(key, _) => key.isEmpty}

            headers=headers++lines.toMap
            if(lines.toMap.keySet.contains("END")){
              isInHeaders=false
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

