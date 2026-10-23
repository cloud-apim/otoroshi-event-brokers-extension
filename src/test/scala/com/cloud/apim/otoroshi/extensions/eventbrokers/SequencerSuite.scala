package com.cloud.apim.otoroshi.extensions.eventbrokers

import java.util.concurrent.{ConcurrentLinkedQueue, Executors}
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

/** what acks and closes go through: one task after the other, whatever the pool */
class SequencerSuite extends munit.FunSuite {

  test("tasks run one after the other, in the order they came, on a pool of many threads") {
    val pool     = Executors.newFixedThreadPool(8)
    val sequence = new Sequencer(ExecutionContext.fromExecutor(pool))
    val seen     = new ConcurrentLinkedQueue[Int]()
    try {
      val all = (0 until 200).map(n => sequence { Thread.sleep(if (n % 7 == 0) 2L else 0L); seen.add(n); n })
      assertEquals(Await.result(Future.sequence(all)(using implicitly, ExecutionContext.global), 30.seconds), (0 until 200).toSeq)
      assertEquals(seen.asScala.toSeq, (0 until 200).toSeq)
    } finally pool.shutdown()
  }

  test("a task that fails does not stop the next ones") {
    val sequence = new Sequencer(ExecutionContext.global)
    val failed   = sequence(throw new IllegalStateException("boom"))
    val next     = sequence("after")
    assert(Await.ready(failed, 5.seconds).value.exists(_.isFailure))
    assertEquals(Await.result(next, 5.seconds), "after")
  }
}
