package helloexoworld.fits.parser.pipeline.stages

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import helloexoworld.fits.parser.pipeline.stages.dataformat._

class HDUParserStage() extends GraphStage[FlowShape[DataBlockWithHeader, DataBlockWithHeader]] {

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

            println("########################################     HDU  block      #####")

            val lines = block.utf8String
              .grouped(80).toList
              .map{s => s.header  }
              .filterNot{case (key,_)=> key.isEmpty}

            headers=headers++lines.toMap

            lines.foreach(println)

            if(lines.toMap.keySet.contains("END")){
              isInHeaders=false
              val dataLength = (
                headers.get("NAXIS1").map(s => s.toInt).getOrElse(0)
                  *  headers.get("NAXIS2").map(s => s.toInt).getOrElse(0))
              val blocIncomplet = if(dataLength%2880 > 0) {1}else{0}
              dataBlockRemining = dataLength / 2880 + blocIncomplet
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
