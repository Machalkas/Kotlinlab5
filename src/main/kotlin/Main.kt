import kotlinx.coroutines.*
import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.exporter.HTTPServer
import kotlinx.coroutines.channels.Channel
import java.io.File
import java.text.SimpleDateFormat
import java.util.*
import kotlin.random.Random


var stop = false
var sensors_stop = false

val sensors_count = Gauge.build().name("count_of_sensors").help("Count of sensors").register()
val sensors_queue = Gauge.build().name("sensor_queue_len").help("Len of sensors queue").register()
val logs_queue = Gauge.build().name("logs_queue_len").help("Len of logs queue").register()
val sensors_time = Histogram.build().name("sensors_time").help("sensors time").register()

val loggingQueue = Channel<LoggerData>(1000)
val sensorQueue = Channel<SensorData>(1000)

val server = HTTPServer.Builder().withPort(4321).build()

fun main() = runBlocking { // this: CoroutineScope
    runSystem()
    server.close()
    println("DONE")
}

suspend fun runSystem() = coroutineScope {
    launch { // launch a new coroutine and continue
        logging(loggingQueue)
    }
    launch {
        host(sensorQueue, loggingQueue)
    }
    repeat(500) {
        launch {
            tempSensor(sensorQueue, 1)
            delay(100)
        }
    }
    launch {
        delay(10000L)
        println("Stop...")
        sensors_stop = true
//        waitForSensors()
        waitForQueue(loggingQueue, sensorQueue)
        stop = true
        println("End main")
    }
}

suspend fun tempSensor(sq: Channel<SensorData>, id: Int, ping: Long = 1000, lifecicle:Int=-1){
//    println("Start sensor")
    val sensorQueue = sq
    val id = id
    val p = ping
    var lc=lifecicle
//    lc=Random.nextInt(1000, 10000)
    sensors_count.inc()
    while (!sensors_stop || lc!=0) {
        sensorQueue.send(SensorData(id, Random.nextInt(1, 100)))
        sensors_queue.inc()
        lc-=1
        delay(1000L)
    }
    sensors_count.dec()
    println("Terning off sensor:$id")
}

suspend fun host(sq: Channel<SensorData>, lq: Channel<LoggerData>){
//    println("Start host")
    val sensorQueue = sq
    val loggingQueue = lq
    var sd: SensorData
    var lg: LoggerData
    while (!stop) {
        delay(1L)
        if (sensorQueue.isEmpty) {
            continue
        }
        sd = sensorQueue.receive()
        sensors_queue.dec()
        sd.freeze()
        lg = LoggerData(sd, sensors_queue.get().toInt())
        loggingQueue.send(lg)
        logs_queue.inc()
    }
    println("Terning off host")
}

suspend fun logging(lq: Channel<LoggerData>){
//    println("Start logger")
    val loggingQueue = lq
    val sdf = SimpleDateFormat("dd/M/yyyy hh:mm:ss")
    val file = File("log.txt").bufferedWriter()
    var lg: LoggerData
    var logMassege: String
    while (!stop) {
        delay(1L)
        if (loggingQueue.isEmpty) {
            continue
        }
        lg = loggingQueue.receive()
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



//fun stopAll(loggingQueue: Channel<LoggerData>, sensorQueue: Channel<SensorData>) {
//    sensors_stop = true
//    waitForQueue(loggingQueue, sensorQueue)
//    stop = true
//}

suspend fun waitForQueue(
    loggingQueue: Channel<LoggerData>,
    sensorQueue: Channel<SensorData>
) {
    while (!loggingQueue.isEmpty || !sensorQueue.isEmpty) {
        println("Waiting for queue to empty: loggingQueue=${logs_queue.get().toInt()}  sensorQueue=${sensors_queue.get().toInt()}")
        delay(1000L)
    }
}

suspend fun waitForSensors(){
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
    fun createLog(lq: Channel<LoggerData>): String =
        "Sensor ${sensorData.id} send value=${sensorData.temp} time=${sensorData.freezeTime} sensorQueue=${sensorLen} | time=${freezeTime} loggingQueue=${logs_queue.get().toInt()}"
}
