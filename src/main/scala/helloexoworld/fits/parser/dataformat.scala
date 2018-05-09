package helloexoworld.fits.parser

import java.nio.ByteBuffer

import akka.util.ByteString
import com.clevercloud.warp10client.{DoubleWarp10Value, IntWarp10Value, LongWarp10Value, Warp10Data}

package object dataformat {

  type DataBlockWithHeader=Tuple3[Boolean, Map[String, String],ByteString]

  implicit class String2Header(val stringValue : String){
    def header = {
        val key = stringValue.take(8)
        val value = stringValue.substring(10) match {
          case s:String if s.startsWith("\'") =>
            val posQuote = s.indexOf('\'', 1)
            s.substring(1, posQuote)
          case s:String if s.indexOf ('/') < 11 => ""
          case s : String => s.substring (10, s.indexOf ('/') )

        }
        key.trim -> value.trim
      }
  }

  case class DataTable(headersWanted : Map[String, String], rowsCount: Int, colsCount : Int, rowLength : Int, fields : List[DataField])
  case class DataField(name: String, fieldIndex: Int, format : FieldFormat, unit : String){
    def fromByteString(bs : ByteBuffer) : DataValue = format match{
      case FloatField => FloatValue(bs.getFloat)
      case DoubleField => DoubleValue(bs.getDouble)
      case IntField => IntValue(bs.getShort)
      case ByteField => ByteValue(bs.get)
      case LongField => LongValue(bs.getInt)
    }
  }

  object DataTable{
    def apply(headers : Map[String, String], _headersWanted : List[String]): DataTable = {
      val colsCount = headers.getOrElse("TFIELDS", "0").toInt
      DataTable(
        headersWanted = headers.filter{case (k,v) => _headersWanted.contains(k)},
        rowsCount= headers.getOrElse("NAXIS2", "0").toInt,
        colsCount = colsCount,
        rowLength = headers.getOrElse("NAXIS1", "0").toInt,
        fields = List.range(1, colsCount+1).map{ i:Int =>
          DataField(
            name=headers.get(s"TTYPE$i").get.replace('\'', ' ').trim,
            fieldIndex = i,
            format = FieldFormat.format(headers.get(s"TFORM$i").get),
            unit = headers.getOrElse(s"TUNIT$i", "Undefined")
          )
        }
      )
    }
  }

  val timestamp0BJD = 2440587.5
  val BJD2timestampShift = 2454833 - timestamp0BJD
  case class DataPoint(labels : Set[(String, String)], index : Int, time : Double, name:String, value:DataValue){
    val time2timestamp:Long = ((time + BJD2timestampShift) * 86400000L).toLong
  }
  case class MetricDataPoint(labels: Set[(String,String)], index : Int, name:String, value:DataValue)

  object DataPoint {
    def toWarp10Data(dataPoint: DataPoint) = {
      def warp10Name(name:String)= name.replace('_', '.').toLowerCase()

      def warp10data(value: DataValue) = value match{
        case data : FloatValue  => DoubleWarp10Value(data.value)
        case data : DoubleValue => DoubleWarp10Value(data.value)
        case data : IntValue    => IntWarp10Value(data.value)
        case data : ByteValue   => IntWarp10Value(data.value)
        case data : LongValue   => LongWarp10Value(data.value)
      }

      val time = dataPoint.time2timestamp * 1000
      Warp10Data(time, None, warp10Name(dataPoint.name), dataPoint.labels, warp10data(dataPoint.value))
    }

    def apply(metricDataPoint: MetricDataPoint, time: Double):DataPoint = DataPoint(
      labels = metricDataPoint.labels,
      index = metricDataPoint.index,
      time= time,
      name = metricDataPoint.name,
      value = metricDataPoint.value
    )

  }
  sealed trait FieldFormat {
    def letter: String
    def length: Int=8
  }
  object FieldFormat{
    def format(letter : String):FieldFormat= letter.replace('\'', ' ').trim match{
      case "B" => ByteField
      case "I" => IntField
      case "J" => LongField
      case "E" => FloatField
      case "D" => DoubleField
    }
  }
  case object ByteField   extends FieldFormat{val letter="B"; override val length=1}
  case object IntField    extends FieldFormat{val letter="I"; override val length=4}
  case object LongField   extends FieldFormat{val letter="J"}
  case object FloatField  extends FieldFormat{val letter="E"; override val length=4}
  case object DoubleField extends FieldFormat{val letter="D"}

  sealed trait DataValue
  case class ByteValue(value : Byte)    extends DataValue
  case class IntValue (value : Int)     extends DataValue
  case class LongValue(value : Long)    extends DataValue
  case class FloatValue(value : Float)  extends DataValue
  case class DoubleValue(value : Double)extends DataValue

}
