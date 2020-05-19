package flink

import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema




object StreamingJob {

  //case class ev(evenType:String,uid:String,timestamp:Long,ip:String,impressionId:String)
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    //Reading from Kafka source
    val displays = env.addSource(new FlinkKafkaConsumer("displays", new SimpleStringSchema(), properties))
    val clicks = env.addSource(new FlinkKafkaConsumer("clicks", new SimpleStringSchema(), properties))


    // extract values from an Array
    def extractValues(arr: Array[String]): (String, Long, String) = {
      val uid = arr(1).split(":")(1)
      val timestamp = arr(2).split(":")(1).toLong
      val ip = arr(3).split(":")(1)
      return (uid,timestamp,ip)
    }
    //FIRST PATTERN:click through rate (CTR)
    //Group by uid and count Clicks
    val uidClick = clicks.map(_.split(","))
      .map(x => (extractValues(x), 1.0)).assignAscendingTimestamps {
      case ((_, timestamp, _), _) => timestamp
    }
      .keyBy { c =>
        c match {
          case ((uid, _, _), _) => uid
        }
      }
      .timeWindow(Time.seconds(15))
      .sum(1)

    //Group by uid and count Diplays
    val uidDisplay = displays.map(_.split(",")).map(x => (extractValues(x), 1.0)).assignAscendingTimestamps {
      case ((_, timestamp, _), _) => timestamp
    }
      .keyBy { c =>
        c match {
          case ((uid, _, _), _) => uid
        }
      }
      .timeWindow(Time.seconds(15))
      .sum(1)

    //join by uid and calcul click through rate (Number of clicks/Number of displays)
    //Normal click rate 10%
    var JoinDataStreamsuid = uidDisplay.join(uidClick).where(x => x._1._1).equalTo(x => x._1._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .apply((d, c) => (c._1._1, c._2 /d._2))
      .filter(x => x._2 > 0.15)
      .print()

    //SECOND PATTERN: Fraudulent Clicks comes from a single Ip adress(high number of clicks from the Ip)
    //Group by ip and count Clicks
    val clickip = clicks.map(_.split(",")).map(x => (extractValues(x), 1.0)) {
      case ((_, timestamp, _), _) => timestamp
    }
      .keyBy { c =>
        c match {
          case ((_, _, ip), _) => ip
        }
      }
      .timeWindow(Time.seconds(10))
      .sum(1)

    //Group by ip and count Diplays
    val displayip = displays.map(_.split(","))
      .map(x => (extractValues(x), 1.0)) {
      case ((_, timestamp, _), _) => timestamp
    }
      .keyBy { c =>
        c match {
          case ((_, _, ip), _) => ip
        }
      }
      .timeWindow(Time.seconds(10))
      .sum(1)
    //join by ip and detect if the number of clicks is up to 10 clicks
    var JoinDataStreamsip = displayip.join(clickip).where(x => x._1._3).equalTo(x => x._1._3)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
      .apply((a, b) => (b._1._3, b._2))
      .filter(x =>(x._2> 10))
      .print()

    //Execution
    env.execute("Flink Kafka")
  }
}
