/**
  * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
  */

package akka.http.impl.engine.client

import akka.http.impl.engine.client.RedirectSupportStage.{InfiniteRedirectLoopException, RedirectMapper}
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, Uri}
import akka.stream._
import akka.stream.scaladsl.BidiFlow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.language.higherKinds
import scala.util.control.NoStackTrace

object RedirectSupportStage {
  type RedirectMapper = (HttpRequest, HttpResponse) => Option[HttpRequest]
  case object InfiniteRedirectLoopException
    extends RuntimeException("Infinite redirect loop detected, breaking.")
      with NoStackTrace
  val defaultRedirectMapper: RedirectMapper =
    (req, resp) =>
      resp.headers.find(_.is("location")).map(location =>
        req.withUri(location.value).withHeaders(List.empty)
      )
  def apply() = BidiFlow.fromGraph(new RedirectSupportStage(defaultRedirectMapper))
  def apply(redirectMapper: RedirectMapper) = BidiFlow.fromGraph(new RedirectSupportStage(redirectMapper))
}

class RedirectSupportStage(redirectMapper: RedirectMapper) extends GraphStage[BidiShape[HttpRequest, HttpRequest, HttpResponse, HttpResponse]] {
  /**
    * in1 is where the original messages come from
    */
  val requestIn: Inlet[HttpRequest] = Inlet("Input1")

  /**
    * in2 is the responses to original messages. If it's a redirect, its not forwarded upstream, but goes downstream
    */
  val responseIn: Inlet[HttpResponse] = Inlet("Input2")

  /**
    * out1 is where input messages go to
    */
  val requestOut: Outlet[HttpRequest] = Outlet("Output1")

  /**
    * out2 is where resulting messages is going to, potentially after redirect
    */
  val responseOut: Outlet[HttpResponse] = Outlet("Output2")

  /**
    * The shape of a graph is all that is externally visible: its inlets and outlets.
    */
  override def shape: BidiShape[HttpRequest, HttpRequest, HttpResponse, HttpResponse] =
    BidiShape(requestIn, requestOut, responseIn, responseOut)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      trait OpOutPrinter[F, _ <: Outlet[F]] {
        def print(t: F): Unit
      }

      trait OpInPrinter[T <: Inlet[_]] {
        def print(): Unit
      }

      def printy(s: String) = ()//println(s)

      object OpOutPrinter {
        implicit val RequestOutPrinterReq = new OpOutPrinter[HttpRequest, Outlet[HttpRequest]] {
          override def print(t: HttpRequest): Unit = printy(s"\tpushing $t to requestOut")
        }
        implicit val ResponseOutPrinterResp = new OpOutPrinter[HttpResponse, Outlet[HttpResponse]] {
          override def print(t: HttpResponse): Unit = printy(s"\tpushing $t to responseOut")
        }
      }

      object OpInPrinter {
        implicit val RequestInPrinterReq = new OpInPrinter[Inlet[HttpRequest]] {
          override def print(): Unit = {
            printy("\tpulling from requestIn")
          }
        }
        implicit val ResponseInPrinterResp = new OpInPrinter[Inlet[HttpResponse]] {
          override def print(): Unit = {
            printy("\tpulling from responseIn")
          }
        }
      }

      import OpOutPrinter._
      import OpInPrinter._

      def doPush[T](outlet: Outlet[T], t: T)(implicit ev: OpOutPrinter[T, Outlet[T]]) = {
        push(outlet, t)
        ev.print(t)
      }

      def doPull[T](inlet: Inlet[T])(implicit ev: OpInPrinter[Inlet[T]]) = {
        if (!isClosed(inlet)) {
          pull(inlet)
        }
        ev.print()
      }

      var inFlight = Vector.empty[HttpRequest]
//      var unconsumedResponses = Vector.empty[HttpResponse]
//      var unconsumedRequests = Vector.empty[HttpRequest]
      var loopDetector = Map.empty[HttpRequest, List[Uri]]
      var requestsProcessed = 0

      def printStatus() = printy(s"=================================================" +
        s"\nIn flight: ${inFlight.length}, processed: $requestsProcessed\n\n")

      /**
        * Invoked before any external events are processed, at the startup of the stage.
        */
      override def preStart(): Unit = {
        printy("preStart")
        doPull(requestIn)
        printStatus()
      }

      /**
        * This is to tell to requestIn that demand exists on requestOut
        */
      setHandler(requestOut, new OutHandler {
        /**
          * Called when the output port has received a pull, and therefore ready to emit an element,
          * i.e. [[GraphStageLogic.push()]] is now allowed to be called on this port.
          */
        override def onPull(): Unit = {
          printy("requestOut: onPull")
          /*unconsumedRequests = unconsumedRequests.headOption match {
            case Some(h) => doPush(requestOut, h); doPull(responseIn); unconsumedRequests.tail
            case None => unconsumedRequests
          }*/
          printStatus()
        }

        /**
          * Called when the output port will no longer accept any new elements. After this callback no other callbacks
          * will be called for this port.
          */
        override def onDownstreamFinish(): Unit = {
          printy("requestOut: onDownstreamFinish")
          cancel(requestIn)
        }

      })

      setHandler(requestIn, new InHandler {
        /**
          * Called when the input port has a new element available. The actual element can be retrieved via the
          * [[GraphStageLogic.grab()]] method.
          */
        override def onPush(): Unit = {
          printy("requestIn: onPush")
          val r = grab(requestIn)
          /*if (isAvailable(requestOut)) {
            doPush(requestOut, r)
            doPull(responseIn)
          } else {
            unconsumedRequests = unconsumedRequests :+ r
          }*/
          emit(requestOut, r, () => doPull(responseIn))
          inFlight = inFlight :+ r
          loopDetector += (r -> List(r.uri))
          printStatus()
        }

        /**
          * Called when the input port is finished. After this callback no other callbacks will be called for this port.
          */
        override def onUpstreamFinish(): Unit = {
          printy("requestIn: onUpstreamFinish")
          complete(requestOut)
        }

        /**
          * Called when the input port has failed. After this callback no other callbacks will be called for this port.
          */
        override def onUpstreamFailure(ex: Throwable): Unit = {
          printy("requestIn: onUpstreamFailure")
          fail(requestOut, ex)
        }
      })

      /**
        * Response is available. Check if it is redirect and act accordingly.
        */
      setHandler(responseIn, new InHandler {
        /**
          * Called when the input port has a new element available. The actual element can be retrieved via the
          * [[GraphStageLogic.grab()]] method.
          */
        override def onPush(): Unit = {
          printy("responseIn: onPush")
          val response = grab(responseIn)
          if (response.status.isRedirection) {
            redirectMapper(inFlight.head, response).foreach { redirect =>
              printy(loopDetector.toString())
              val loops = loopDetector(inFlight.head)
              if (loops.contains(redirect.uri)) {
                throw InfiniteRedirectLoopException
              } else {
                loopDetector -= inFlight.head
                loopDetector += (redirect -> (redirect.uri :: loops))
                inFlight = inFlight.tail :+ redirect
                emit(requestOut, redirect, () => doPull(responseIn))
                /*if (isAvailable(requestOut)) {
                  doPush(requestOut, redirect)
                  doPull(responseIn)
                } else {
                  unconsumedRequests = unconsumedRequests :+ redirect
                }*/
              }
            }
          } else {
            requestsProcessed += 1
            loopDetector -= inFlight.head
            inFlight = inFlight.tail
            emit(responseOut, response, () => doPull(requestIn))
            /*if (isAvailable(responseOut)) {
              doPush(responseOut, response)
              doPull(requestIn)
            } else {
              unconsumedResponses = unconsumedResponses :+ response
            }*/
          }
          printStatus()
        }

        /**
          * Called when the input port is finished. After this callback no other callbacks will be called for this port.
          */
        override def onUpstreamFinish(): Unit = {
//          emitMultiple(responseOut, unconsumedResponses)
          printy("responseIn: onUpstreamFinish")
          complete(responseOut)
        }

        /**
          * Called when the input port has failed. After this callback no other callbacks will be called for this port.
          */
        override def onUpstreamFailure(ex: Throwable): Unit = {
          printy("responseIn: onUpstreamFailure")
          fail(responseOut, ex)
        }
      })

      setHandler(responseOut, new OutHandler {
        /**
          * Called when the output port has received a pull, and therefore ready to emit an element,
          * i.e. [[GraphStageLogic.push()]] is now allowed to be called on this port.
          */
        override def onPull(): Unit = {
          printy("responseOut: onPull")
          /*unconsumedResponses = unconsumedResponses.headOption match {
            case Some(h) => doPush(responseOut, h); doPull(requestIn); unconsumedResponses.tail
            case None => unconsumedResponses
          }*/
          printStatus()
        }

        /**
          * Called when the output port will no longer accept any new elements. After this callback no other callbacks will
          * be called for this port.
          */
        override def onDownstreamFinish(): Unit = {
          printy("responseOut: onDownstreamFinish")
          cancel(responseIn)
        }
      })

    }
}