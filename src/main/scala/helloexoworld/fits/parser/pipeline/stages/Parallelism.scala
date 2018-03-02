package helloexoworld.fits.parser.pipeline.stages

import akka.NotUsed
import akka.stream.{FlowShape, Graph}
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Merge, Source}

object Parallelism {
  def balancer[In, Out](workerFlow: Graph[FlowShape[In, Out], Any], workerCount: Int): Flow[In, Out, NotUsed] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      val balancer = b.add(Balance[In](workerCount, waitForAllDownstreams = true))
      val merge = b.add(Merge[Out](workerCount))

      for (i <- 1 to workerCount) {
        // for each worker, add an edge from the balancer to the worker, then wire
        // it to the merge element
        val worker = b.add(Flow.fromGraph(workerFlow))
        balancer ~> worker ~> merge
      }

      FlowShape(balancer.in, merge.out)
    })
  }

}
