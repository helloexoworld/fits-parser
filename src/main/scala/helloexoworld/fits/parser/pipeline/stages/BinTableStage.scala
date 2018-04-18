package helloexoworld.fits.parser.pipeline.stages

import java.nio.ByteBuffer

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import helloexoworld.fits.parser.pipeline.stages.dataformat._

class BinTableStage(val filterList : List[String], val headersWanted : List[String]) extends GraphStage[FlowShape[DataBlockWithHeader, DataPoint]] {

  val in = Inlet[DataBlockWithHeader]("BintableParser.in")
  val out = Outlet[DataPoint]("BintableParser.out")

  override def shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var naxisDimSize = List.empty
      var dataBlockRemining = 0

      var dataPointIndex = 0

      var optDataTable : Option[DataTable] = None

      var reste : ByteString = ByteString("")

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val (isNewFile, headers, block) = grab(in)
          headers.get("XTENSION") match {
            case Some("BINTABLE") =>
              if(isNewFile){
                reste= ByteString("")
                optDataTable = Some(DataTable(headers, headersWanted))
                dataPointIndex=0
              }
           //   println("########################################   BINTABLE  block      #####")
          //    println(headers)
              val buffer = reste.concat(block)
              reste = ByteString("")
              val dataTable = optDataTable.get
              val liste = buffer
                .grouped(dataTable.rowLength)
                .filter(bs => bs.length==dataTable.rowLength)
                .map{s =>
                  val bs = s.asByteBuffer
                  dataPointIndex = dataPointIndex + 1
                  val time = dataTable.fields.head.fromByteString(bs).asInstanceOf[DoubleValue].value
                 // if(!time.isNaN) {
                    val metrics = dataTable.fields.drop(1).flatMap { f =>
                      val value = f.fromByteString(bs)
                      if (filterList.contains(f.name)) {
                        Some(MetricDataPoint(dataTable.headersWanted, dataPointIndex, f.name, value))

                      } else {
                        None
                      }
                    }
                    metrics.map(m => DataPoint(m, time))
                //  }else{Seq.empty}
                }
                .flatten
                .filter{dp => dp.index <= dataTable.rowsCount}

              buffer
                .grouped(dataTable.rowLength)
                .filterNot(bs => bs.length==dataTable.rowLength)
                .foreach(bs => reste = bs )

              emitMultiple(out, liste, ()=>{})

            case _ =>
             // println("drop non bintable block")
              pull(in)
          }
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = { pull(in) }
      })
    }
}
