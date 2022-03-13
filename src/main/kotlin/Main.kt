import kotlinx.coroutines.*
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.exporter.HTTPServer
import java.io.File
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import kotlin.random.Random


var stop = false
var sensors_stop = false

val sensors_count = Gauge.build().name("count_of_sensors").help("Count of sensors").register()
val sensors_queue = Gauge.build().name("sensor_queue_len").help("Len of sensors queue").register()
val logs_queue = Gauge.build().name("logs_queue_len").help("Len of logs queue").register()
val sensors_time = Histogram.build().name("sensors_time").help("sensors time").register()


fun main() = runBlocking { // this: CoroutineScope
    launch { // launch a new coroutine and continue
        delay(1000L) // non-blocking delay for 1 second (default time unit is ms)
        println("World!") // print after delay
    }
    println("Hello") // main coroutine continues while a previous one is delayed
}




private fun stopAll(loggingQueue: ArrayBlockingQueue<LoggerData>, sensorQueue: ArrayBlockingQueue<SensorData>) {
    sensors_stop = true
    waitForQueue(loggingQueue, sensorQueue)
    stop = true
}

private fun waitForQueue(
    loggingQueue: ArrayBlockingQueue<LoggerData>,
    sensorQueue: ArrayBlockingQueue<SensorData>
) {
    while (loggingQueue.isNotEmpty() || sensorQueue.isNotEmpty()) {
        println("Waiting for queue to empty: loggingQueue=${loggingQueue.count()}  sensorQueue=${sensorQueue.count()}")
        Thread.sleep(1000)
    }
}

private fun waitForSensors(){
    while (sensors_count.get()>0){
    }
}



open class ThreadData(val timestamp: Long = System.currentTimeMillis()) {
    var freezeTime: Long = -1
    open fun freeze() {
        freezeTime = System.currentTimeMillis() - timestamp
    }
}
data class SensorData(val id: Int, val temp: Int) : ThreadData(){
    val timest=sensors_time.startTimer()
    override fun freeze() {
        timest.observeDuration()
        super.freeze()
    }
}
data class LoggerData(val sensorData: SensorData, val sensorLen: Int) : ThreadData() {
    fun createLog(lq: ArrayBlockingQueue<LoggerData>): String =
        "Sensor ${sensorData.id} send value=${sensorData.temp} time=${sensorData.freezeTime} sensorQueue=${sensorLen} | time=${freezeTime} loggingQueue=${lq.count()}"
}
