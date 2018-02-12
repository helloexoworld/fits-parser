package helloexoworld.fits.parser.pipeline.stages

import akka.Done
import akka.stream._
import akka.stream.scaladsl.{Flow, Sink}
import akka.stream.stage.{GraphStage, GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import helloexoworld.fits.parser.pipeline.stages.dataformat.{ByteValue, DataPoint, DataValue, DoubleValue, FloatValue, IntValue, LongValue}
import com.clevercloud.warp10client._
import org.http4s.Uri

import scala.concurrent.Future

class Upload2Warp10Stage (uri:Uri, writeToken:String, dataSetSize:Int=1000) extends GraphStageWithMaterializedValue[SinkShape[DataPoint], Future[Done]] {

  val in = Inlet[DataPoint]("Upload2Warp10.in")

  override def shape = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      val w10client = new Warp10Client(uri, writeToken)
      var dataset : Set[Warp10Data] = Set.empty
      setHandler(in, new InHandler {

        override def onPush(): Unit = {
          val dataPoint = grab(in)
          val time = dataPoint.time2timestamp*1000

          val point = Warp10Data(time, None, warp10Name(dataPoint.name), Set("objectId" -> dataPoint.objectId), warp10data(dataPoint.value))
          
          dataset = dataset + point
          if(dataset.size >= dataSetSize){
            w10client.sendData(dataset).unsafePerformSyncAttempt.fold(error => failStage(error), x=>{println(x);pull(in)})
            dataset = Set.empty
          }else{
            pull(in)
          }
        }
        
        override def onUpstreamFinish():Unit={
          w10client.sendData(dataset).unsafePerformSyncAttempt.fold(error => failStage(error), x=>println(x))
        }
      })

      override def preStart(): Unit = pull(in)
                                                              
    }

  def warp10Name(name:String)= name.replace('_', '.').toLowerCase()

  def warp10data(value: DataValue) = value match{
    case data : FloatValue  => DoubleWarp10Value(data.value)
    case data : DoubleValue => DoubleWarp10Value(data.value)
    case data : IntValue    => IntWarp10Value(data.value)
    case data : ByteValue   => IntWarp10Value(data.value)
    case data : LongValue   => LongWarp10Value(data.value)
  }

}
