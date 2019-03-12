package me.jrwang.app

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

import me.jrwang.aloha.app.{AbstractApplication, ExitCode, ExitState}

class SimpleContextThreadApp extends AbstractApplication {

  val workThread: Thread = new Thread(new Runnable {
    override def run(): Unit = {
      try {
        1 to 100 foreach { _ =>
          println("...")
          Thread.sleep(1000)
        }
        result.success(ExitState(ExitCode.SUCCESS, Some("success")))
      } catch {
        case _: InterruptedException =>
          result.success(ExitState(ExitCode.FAILED, Some("killed")))
      }
    }
  })

  override def start(): Promise[ExitState] = {
    println("Start job.")
    workThread.start()
    result
  }

  override def shutdown(reason: Option[String]): Unit = {
    println("Kill job.")
    workThread.interrupt()
  }

  override def clean(): Unit = {}
}

object SimpleContextThreadApp {
  def main(args: Array[String]): Unit = {
    val job = new SimpleContextThreadApp

    val result = job.start()

    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(10000)
        job.shutdown(Some("No reason"))
      }
    }).start()

    try {
      val a  = Await.result(result.future, Duration.Inf)
      println(a)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
    }
  }
}