import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.{ProcessorContext, PunctuationType}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore, StoreBuilder, Stores}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig, Topology}

import java.time.Duration
import java.util.{Locale, Properties}


class WordCountProcessor extends Processor[String, String, String, String]{

  var kvStore: KeyValueStore[String, Int] = null

  def init (context: ProcessorContext): Unit = {
    context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, _ => {
      val iteration: KeyValueIterator[String, Int] = kvStore.all()
        while (iteration.hasNext) {
          val entry: KeyValue [String, Int] = iteration.next()
          context.forward(entry.key, entry.value.toString)
        }
      })
    kvStore = context.getStateStore("Counts")
  }

  def process (record: Record): Unit = {
    val words: String = record.value().toLowerCase(Locale.getDefault()).split("\\W+")
    for (word <- words) {
      val oldValue: Int = kvStore.get(word)
      if (oldValue == 0) kvStore.put(word, 1) else kvStore.put(word, oldValue + 1)
    }
  }

  def close(): Unit
}

class EnrichProcessor extends Processor[String, String] {

  var context: ProcessorContext

  def init (context: ProcessorContext): Unit = {
    this.context = context
  }

}

val countStoreSupplier: StoreBuilder[KeyValueStore[String, Long]] = Stores.keyValueStoreBuilder(
  Stores.persistentKeyValueStore("Counts"),
  Serdes.stringSerde,
  Serdes.longSerde)
  .withLoggingDisabled()

val topology: Topology = new Topology

topology.addSource("Source", "source-topic")
  .addProcessor("Process", () => new WordCountProcessor(), "Source")
  .addStateStore(countStoreSupplier, "Process")
  .addSink("Sink","sink-topic","Process")


val properties: Properties = new Properties()
properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-api-application")
properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")


val KafkaStreams = new KafkaStreams(topology, properties)
KafkaStreams.start()
sys.ShutdownHookThread {
  KafkaStreams.close(Duration.ofSeconds(10))
}