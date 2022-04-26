import org.apache.kafka.streams.kstream.ValueTransformerWithKey
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier
import org.apache.kafka.streams.processor.{ProcessorContext, api}
import org.apache.kafka.streams.processor.api.{Processor, ProcessorSupplier}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import domain.{Key, Stocks}

abstract class ProcessorApi extends ProcessorSupplier[Key, Stocks, Key, Stocks] {

  class Supplier(var storeName: String) {
    this.storeName = storeName

    var store: KeyValueStore[Key, Stocks] = null

    def init(context: ProcessorContext): Unit = {
      this.store = context
        .getStateStore(storeName)
        .asInstanceOf[KeyValueStore[Key, Stocks]]
    }

    def process(record: Record): Unit = ()

    def close: Unit = ()

  }
}

val storeName: String = "store_1"
val storeBuilder = new Stores.keyValueStoreBuilder

 // var state: KeyValueStore[PartitioningKey,StockAggregate] = null
 // def init(context: ProcessorContext): Unit = {
  //  this.state = context
  //    .getStateStore(storeName)
  //    .asInstanceOf[KeyValueStore[PartitioningKey,StockAggregate]]
  //}
 // def transform(k: PartitioningKey, v: DeltaAggOutput): StockAggOutput = {
  //  val agg = Option(state.get(k)) match {
  //    case None => StockAggregate.init(k)
  //    case Some(a) => a
  //  }
  //  val newAgg = agg.update(v)
  //  newAgg.group.isEmpty match {
   //   case true => state.delete(k)
   //   case false => state.put(k, newAgg)
   // }
   // newAgg.result
 // }
  //def close: Unit = ()



//object StockAggregator {

  //def supplier(stateStoreName: String) = new ValueTransformerWithKeySupplier[PartitioningKey,DeltaAggOutput,StockAggOutput]() {
   // def get: ValueTransformerWithKey[PartitioningKey,DeltaAggOutput,StockAggOutput] = new StockAggregator(stateStoreName)
 // }

