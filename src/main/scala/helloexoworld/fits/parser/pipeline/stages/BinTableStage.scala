package helloexoworld.fits.parser.pipeline.stages

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import helloexoworld.fits.parser.dataformat._

class BinTableStage(val filterList : List[String], val headersWanted : List[String]) extends GraphStage[FlowShape[DataBlockWithHeader, DataPoint]] {

  val in = Inlet[DataBlockWithHeader]("BintableParser.in")
  val out = Outlet[DataPoint]("BintableParser.out")

  override def shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      var naxisDimSize = List.empty
      var dataBlockRemining = 0

      var dataPointIndex = 0
      var fields : List[DataField] = List.empty

      var dataTable : DataTable=_

      var headersWantedSet:Set[(String, String)]=Set.empty

      var reste : ByteString = ByteString("")

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val (isNewFile, headers, block) = grab(in)
          headers.get("XTENSION") match {
            case Some("BINTABLE") =>
              if (isNewFile) {
                reste = ByteString("")
                dataTable = DataTable(headers, headersWanted)
                dataPointIndex = 0
                headersWantedSet = dataTable.headersWanted.toSet
                fields = dataTable.fields.drop(1) // TIME is always the first data
              }
              //   println("########################################   BINTABLE  block      #####")
              //    println(headers)
              val buffer = reste.concat(block)
              reste = ByteString("")
              val liste = buffer
                .grouped(dataTable.rowLength)
                .filter(bs => bs.length == dataTable.rowLength)
                .flatMap { s =>
                  if (dataPointIndex >= dataTable.rowsCount) {
                    None
                  } else {
                    val bs = s.asByteBuffer
                    dataPointIndex = dataPointIndex + 1
                    val time = bs.getDouble // timeField.fromByteString(bs).asInstanceOf[DoubleValue].value
                    fields.flatMap { f =>
                      val value = f.fromByteString(bs)
                      if (filterList.contains(f.name))
                        Some(
                          DataPoint(
                            labels = headersWantedSet,
                            index = dataPointIndex,
                            time = time,
                            name = f.name,
                            value = value
                          )
                        )
                      else
                        None
                    }
                  }
                }

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
