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

val loggingQueue = ArrayBlockingQueue<LoggerData>(1000, true)
val sensorQueue = ArrayBlockingQueue<SensorData>(1000, true)

fun main() = runBlocking { // this: CoroutineScope
    runSystem()
    println("DONE")
}

suspend fun runSystem() = coroutineScope {
    launch { // launch a new coroutine and continue
        logging(loggingQueue)
    }
    launch {
        host(sensorQueue, loggingQueue)
    }
    repeat(3) {
        launch {
            tempSensor(sensorQueue, 1)
        }
    }
    launch {
        delay(5000L)
        println("Stop...")
        sensors_stop = true
        waitForQueue(loggingQueue, sensorQueue)
        stop = true
        println("End main")
    }
}

suspend fun tempSensor(sq: ArrayBlockingQueue<SensorData>, id: Int, ping: Long = 1000, lifecicle:Int=-1){
//    println("Start sensor")
    val sensorQueue = sq
    val id = id
    val p = ping
    var lc=lifecicle
    sensors_count.inc()
    while (!sensors_stop && lc!=0) {
        sensorQueue.put(SensorData(id, Random.nextInt(1, 100)))
        sensors_queue.inc()
        lc-=1
        delay(1000L)
    }
    sensors_count.dec()
    println("Terning off sensor:$id")
}

suspend fun host(sq: ArrayBlockingQueue<SensorData>, lq: ArrayBlockingQueue<LoggerData>){
//    println("Start host")
    val sensorQueue = sq
    val loggingQueue = lq
    var sd: SensorData
    var lg: LoggerData
    while (!stop) {
        delay(1L)
        if (sensorQueue.isEmpty()) {
            continue
        }
        sd = sensorQueue.take()
        sensors_queue.dec()
        sd.freeze()
        lg = LoggerData(sd, sensorQueue.count())
        loggingQueue.put(lg)
        logs_queue.inc()
    }
    println("Terning off host")
}

suspend fun logging(lq: ArrayBlockingQueue<LoggerData>){
//    println("Start logger")
    val loggingQueue = lq
    val sdf = SimpleDateFormat("dd/M/yyyy hh:mm:ss")
    val file = File("log.txt").bufferedWriter()
    var lg: LoggerData
    var logMassege: String
    while (!stop) {
        delay(1L)
        if (loggingQueue.isEmpty()) {
            continue
        }
        lg = loggingQueue.take()
        logs_queue.dec()
        lg.freeze()
        logMassege =
            "${lg.createLog(loggingQueue)} || ${sdf.format(Date())}\n"
        print(logMassege)
        file.write(logMassege)
    }
    file.close()
    println("Terning off logger")
}



//fun stopAll(loggingQueue: ArrayBlockingQueue<LoggerData>, sensorQueue: ArrayBlockingQueue<SensorData>) {
//    sensors_stop = true
//    waitForQueue(loggingQueue, sensorQueue)
//    stop = true
//}

suspend fun waitForQueue(
    loggingQueue: ArrayBlockingQueue<LoggerData>,
    sensorQueue: ArrayBlockingQueue<SensorData>
) {
    while (loggingQueue.isNotEmpty() || sensorQueue.isNotEmpty()) {
        println("Waiting for queue to empty: loggingQueue=${loggingQueue.count()}  sensorQueue=${sensorQueue.count()}")
        delay(1000L)
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
