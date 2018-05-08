package helloexoworld.fits.parser.pipeline.stages

import akka.stream._
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import com.clevercloud.warp10client._
import org.http4s.Uri

import scala.concurrent.{Future, Promise}

class Upload2Warp10Stage (uri:Uri, writeToken:String, dataSetSize:Int=1000) extends GraphStageWithMaterializedValue[SinkShape[Warp10Data], Future[IOResult]] {

  val in = Inlet[Warp10Data]("Upload2Warp10.in")

  override def shape = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes) = {
    val result = Promise[IOResult]()
    val logic = new GraphStageLogic(shape) {
      val w10client = new Warp10Client(uri, writeToken)
      var dataset: Set[Warp10Data] = Set.empty
      var dataPointCount =0
      setHandler(in, new InHandler {

        override def onPush(): Unit = {
          val point = grab(in)

          dataset = dataset + point
          if (dataset.size >= dataSetSize) {
            w10client.sendData(dataset).unsafePerformSyncAttempt.fold(error => failStage(error), x => {
              dataPointCount += dataset.size
              println(x);
              pull(in)
            })
            dataset = Set.empty
          } else {
            pull(in)
          }
        }

        override def onUpstreamFinish(): Unit = {
          w10client.sendData(dataset).unsafePerformSyncAttempt.fold(
            error => {
              failStage(error)
              println(error)
              result.trySuccess(IOResult.createFailed(dataPointCount, error))
            },
            x => {
              dataPointCount += dataset.size
              println(x)
              result.trySuccess(IOResult.createSuccessful(dataPointCount))
            })
        }
      })

      override def preStart(): Unit = pull(in)

      override def postStop(): Unit = w10client.closeNow

    }
    (logic, result.future)
  }

}
