package com.cloud.apim.otoroshi.extensions.eventbrokers.clients

import com.cloud.apim.otoroshi.extensions.eventbrokers.*
import com.cloud.apim.otoroshi.extensions.eventbrokers.entities.{EventBrokerConnection, RedisSettings}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.{Consumer, Limit, Range, StreamMessage, XAddArgs, XAutoClaimArgs, XGroupCreateArgs, XReadArgs}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import otoroshi.env.Env
import otoroshi.storage.drivers.lettuce.ByteStringRedisCodec
import otoroshi.utils.syntax.implicits.*
import play.api.libs.json.{JsObject, JsString, Json}

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Redis streams: an append-only log. XADD publishes an entry, the attributes of the event in `ce_`
 * fields next to the data field. A subscription has a connection of its own, its reads block:
 *  - without a group, XREAD from where it starts (from now on, the beginning, a date, an entry id):
 *    every subscriber gets every entry
 *  - with a group, XREADGROUP: the members of the group share the entries. an entry is XACKed as it
 *    leaves the subscription (at most once) or once handed over (at least once), and what a member read
 *    and left unacked for `claim_min_idle_ms` (it went away, it was cut) goes to another one (XAUTOCLAIM)
 *
 * A read asks for `prefetch` entries (100 by default): in a group, what a member read is its own until
 * acked, a smaller prefetch shares the entries more evenly between slow members.
 */
class RedisStreamsBrokerClient(val config: EventBrokerConnection, settings: RedisSettings) extends RedisClientSupport(settings) with EventBrokerClient {

  private val streams = settings.streams

  /** how long a read waits for entries: a subscription cancelled is let go after that long at most */
  private val BlockFor = 2000L

  private def prefetch(spec: SubscriptionSpec): Either[String, Long] =
    spec.options.select("prefetch").asOpt[String].orElse(spec.options.select("prefetch").asOpt[Long].map(_.toString)) match {
      case None        => Right(100L)
      case Some(value) => value.trim.toLongOption.filter(n => n >= 1 && n <= 10000).toRight(s"prefetch is a number between 1 and 10000, not '$value'")
    }

  private val logger = play.api.Logger("cloud-apim-event-brokers-redis-streams")

  private val connectionRef = new AtomicReference[Option[StatefulRedisConnection[String, ByteString]]](None)

  private def connection: StatefulRedisConnection[String, ByteString] =
    connectionRef.get().getOrElse(throw new IllegalStateException("the redis client is not started"))

  override val capabilities: BrokerCapabilities = BrokerCapabilities(acks = true, groups = true, replay = true, keys = false, headers = true)

  override def start()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    connectionRef.set(Some(startClient().connect(new ByteStringRedisCodec())))
  }

  override def healthcheck()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    connection.sync().ping()
    ()
  }

  def close()(using env: Env, ec: ExecutionContext): Future[Unit] = Future {
    connectionRef.getAndSet(None).foreach(c => Try(c.close()))
    shutdownClient()
  }

  private def noStream = new IllegalArgumentException("no stream: none in the call, and the connection has no streams.default_stream")

  private val RoutingKeys = Seq("stream", "topic", "channel")

  override def publish(events: Seq[BrokerMessage], opts: JsObject)(using env: Env, ec: ExecutionContext): Future[Seq[PublishResult]] = {
    val commands = connection.async()
    Future.sequence(events.map { event =>
      try {
        val stream = RoutingKeys
          .flatMap(event.metadata.get)
          .headOption
          .orElse(RoutingKeys.flatMap(key => opts.select(key).asOpt[String]).headOption)
          .orElse(streams.defaultStream)
          .getOrElse(throw noStream)
        val fields = new java.util.LinkedHashMap[String, ByteString]()
        event.headers.foreach((name, value) => if (name != streams.dataField) fields.put(name, ByteString(value)))
        fields.put(streams.dataField, event.data)
        val added  = streams.maxLength match {
          case None      => commands.xadd(stream, fields)
          case Some(max) => commands.xadd(stream, new XAddArgs().maxlen(max).approximateTrimming(!streams.exactTrimming), fields)
        }
        added.asScala
          .map(id => PublishResult.ok(Json.obj("stream" -> stream, "id" -> id)))
          .recover { case NonFatal(e) => PublishResult.failed(e) }
      } catch {
        case NonFatal(e) => Future.successful(PublishResult.failed(e))
      }
    })
  }

  /** an entry: its data field is the data, its other fields the headers. without one, all its fields as json */
  private def toMessage(entry: StreamMessage[String, ByteString], extra: Map[String, String]): BrokerMessage = {
    val body     = Option(entry.getBody).map(_.asScala.toSeq).getOrElse(Seq.empty)
    val metadata = Map("provider" -> "redis", "stream" -> entry.getStream, "id" -> entry.getId) ++ extra
    body.find(_._1 == streams.dataField) match {
      case Some((_, data)) =>
        BrokerMessage(data = data, headers = body.filterNot(_._1 == streams.dataField).map((name, value) => name -> value.utf8String).toMap, metadata = metadata)
      // an entry added by another producer, the redis way
      case None            =>
        BrokerMessage(
          data = ByteString(Json.stringify(JsObject(body.map((name, value) => name -> JsString(value.utf8String))))),
          headers = Map("content-type" -> "application/json"),
          metadata = metadata
        )
    }
  }

  private val EntryId = """^(\d+)(?:-(\d+))?$""".r

  /** the id right before `id`: a read delivers what comes after the id it is given */
  private def before(id: String): Either[String, String] = id.trim match {
    case EntryId(millis, sequence) =>
      val ms  = BigInt(millis)
      val seq = Option(sequence).map(BigInt(_)).getOrElse(BigInt(0))
      if (seq > 0) Right(s"$ms-${seq - 1}")
      else if (ms > 0) Right(s"${ms - 1}-18446744073709551615")
      else Right("0-0")
    case other                     => Left(s"from_id is a stream entry id (<millis>-<sequence>), not '$other'")
  }

  /** where a subscription starts in a stream: the id right before its first entry. none: from now on */
  private def startOf(spec: SubscriptionSpec, stream: String): Either[String, Option[String]] = {
    val fromId = spec.options.select("from_id").asOpt[String].map(_.trim).filter(_.nonEmpty)
    (spec.from, fromId) match {
      case (StartFrom.Positions(positions), _) => Right(positions.get(stream))
      case (_, Some(id))                       => before(id).map(Some(_))
      case (StartFrom.Earliest, _)             => Right(Some("0-0"))
      case (StartFrom.Timestamp(millis), _)    => before(millis.toString).map(Some(_))
      case (StartFrom.Offset(_), _)            => Left("redis streams have no offsets: start from an entry id (from_id), or from a date (from_timestamp)")
      case (StartFrom.Latest, _)               => Right(None)
    }
  }

  override def subscribe(spec: SubscriptionSpec)(using env: Env, ec: ExecutionContext): Future[Source[ReceivedEvent, ?]] = {
    val names  = if (spec.topics.isEmpty) streams.defaultStream.toSeq else spec.topics.flatMap(_.split(",").map(_.trim)).filter(_.nonEmpty).distinct
    val starts = names.map(name => startOf(spec, name).map(name -> _))
    val count  = prefetch(spec)
    if (names.isEmpty) Future.failed(noStream)
    else if (spec.group.isEmpty && spec.delivery == DeliveryMode.AtLeastOnce)
      Future.failed(new IllegalArgumentException("redis streams acknowledge entries within a group: at-least-once delivery needs a group"))
    else
      (starts.collectFirst { case Left(error) => error } ++ count.left.toOption).headOption match {
        case Some(error) => Future.failed(new IllegalArgumentException(error))
        case None        =>
          val from = starts.collect { case Right(start) => start }
          // its own connection: a blocked read holds it
          client.connectAsync(new ByteStringRedisCodec(), redisUri).asScala.flatMap { reader =>
            val opened = spec.group match {
              case None        => read(reader, from, count.getOrElse(100L))
              case Some(group) => readGroup(reader, group, from, count.getOrElse(100L), spec)
            }
            opened.recoverWith { case NonFatal(e) =>
              reader.closeAsync()
              Future.failed(e)
            }
          }
      }
  }

  private def read(reader: StatefulRedisConnection[String, ByteString], from: Seq[(String, Option[String])], count: Long)(using
      env: Env,
      ec: ExecutionContext
  ): Future[Source[ReceivedEvent, ?]] = {
    val commands = reader.async()
    // from now on: from the last entry there is now, not from what is last at each read
    val resolved = Future.sequence(from.map {
      case (stream, Some(id)) => Future.successful(stream -> id)
      case (stream, None)     =>
        commands.xrevrange(stream, Range.create("-", "+"), Limit.create(0, 1)).asScala.map(last => stream -> last.asScala.headOption.map(_.getId).getOrElse("0-0"))
    })
    resolved.map { positions =>
      val entries = Source
        .unfoldAsync(positions.toMap) { cursor =>
          commands
            .xread(XReadArgs.Builder.block(BlockFor).count(count), cursor.toSeq.map((stream, id) => XReadArgs.StreamOffset.from(stream, id))*)
            .asScala
            .map { batch =>
              val read = Option(batch).map(_.asScala.toSeq).getOrElse(Seq.empty)
              Some((read.foldLeft(cursor)((c, entry) => c + (entry.getStream -> entry.getId)), read.map(entry => ReceivedEvent.unacked(toMessage(entry, Map.empty)))))
            }
        }
        .mapConcat(identity)
      Subscribed(entries)(() => { reader.closeAsync(); () })
    }
  }

  /**
   * where a member of a group is: still going through what it had pending (a named consumer coming back),
   * where each stream is in the current claim round, and when the last round ended
   */
  private final case class GroupCursor(pending: Option[Map[String, String]], claims: Map[String, String], lastClaim: Long)

  private def busyGroup(e: Throwable): Boolean =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).exists(t => Option(t.getMessage).exists(_.contains("BUSYGROUP")))

  private def readGroup(
      reader: StatefulRedisConnection[String, ByteString],
      group: String,
      from: Seq[(String, Option[String])],
      count: Long,
      spec: SubscriptionSpec
  )(using
      env: Env,
      ec: ExecutionContext
  ): Future[Source[ReceivedEvent, ?]] = {
    val commands = reader.async()
    // acks, and what must not wait behind a blocked read
    val shared   = connection.async()
    val named    = spec.options.select("consumer").asOpt[String].map(_.trim).filter(_.nonEmpty)
    val consumer = Consumer.from(group, named.getOrElse(s"otoroshi-${UUID.randomUUID().toString.replace("-", "").take(12)}"))
    val acks     = spec.delivery == DeliveryMode.AtLeastOnce
    val names    = from.map(_._1)
    val claiming = streams.claimMinIdleMs > 0
    val claimGap = (streams.claimMinIdleMs / 2).max(100L).min(30000L)
    // read and not acked yet: what a claim round must not deliver a second time
    val inFlight = ConcurrentHashMap.newKeySet[String]()

    def received(entry: StreamMessage[String, ByteString], again: Boolean): ReceivedEvent = {
      val message = toMessage(entry, Map("group" -> group, "consumer" -> consumer.getName) ++ (if (again) Map("redelivered" -> "true") else Map.empty))
      val key     = s"${entry.getStream}/${entry.getId}"
      inFlight.add(key)
      new ReceivedEvent(message, () => shared.xack(entry.getStream, group, entry.getId).asScala.map(_ => { inFlight.remove(key); () }))
    }

    // an entry deleted from the stream while pending comes back empty: nothing to deliver, done with it
    def deliverable(entries: Seq[StreamMessage[String, ByteString]]): Seq[StreamMessage[String, ByteString]] = {
      val (gone, there) = entries.partition(_.getBody == null)
      gone.foreach(entry => shared.xack(entry.getStream, group, entry.getId))
      there
    }

    def step(cursor: GroupCursor): Future[Option[(GroupCursor, Seq[ReceivedEvent])]] = cursor.pending match {
      // what this consumer got before and never acked, first
      case Some(pending) =>
        commands
          .xreadgroup(consumer, XReadArgs.Builder.count(count), pending.toSeq.map((stream, id) => XReadArgs.StreamOffset.from(stream, id))*)
          .asScala
          .map { batch =>
            val read = Option(batch).map(_.asScala.toSeq).getOrElse(Seq.empty)
            if (read.isEmpty) Some((cursor.copy(pending = None), Seq.empty))
            else Some((cursor.copy(pending = Some(read.foldLeft(pending)((p, entry) => p + (entry.getStream -> entry.getId)))), deliverable(read).map(received(_, again = true))))
          }
      // what another member left unacked for too long
      case None if claiming && System.currentTimeMillis() - cursor.lastClaim >= claimGap =>
        Future
          .sequence(names.map { stream =>
            commands
              .xautoclaim(stream, XAutoClaimArgs.Builder.xautoclaim(consumer, streams.claimMinIdleMs, cursor.claims.getOrElse(stream, "0-0")).count(count))
              .asScala
              .map(claimed => (stream, claimed))
          })
          .map { results =>
            val next    = results.map((stream, claimed) => stream -> Option(claimed.getId).getOrElse("0-0")).toMap
            val entries = results.flatMap((_, claimed) => Option(claimed.getMessages).map(_.asScala.toSeq).getOrElse(Seq.empty))
            // a cursor back at the start: the round is over, the next one waits
            val over    = next.values.forall(_ == "0-0")
            val now     = System.currentTimeMillis()
            Some((
              cursor.copy(claims = if (over) Map.empty else next, lastClaim = if (over) now else cursor.lastClaim),
              deliverable(entries).filterNot(entry => inFlight.contains(s"${entry.getStream}/${entry.getId}")).map(received(_, again = true))
            ))
          }
      case None          =>
        commands
          .xreadgroup(consumer, XReadArgs.Builder.block(BlockFor).count(count), names.map(stream => XReadArgs.StreamOffset.lastConsumed(stream))*)
          .asScala
          .map(batch => Some((cursor, Option(batch).map(_.asScala.toSeq).getOrElse(Seq.empty).map(received(_, again = false)))))
    }

    // the group starts where the subscription asks for when it does not exist yet. once it does, it knows
    // where it is, and the start asked for does not matter
    val created = Future.sequence(from.map { (stream, start) =>
      shared
        .xgroupCreate(XReadArgs.StreamOffset.from(stream, start.getOrElse("$")), group, XGroupCreateArgs.Builder.mkstream(true))
        .asScala
        .map(_ => ())
        .recover { case NonFatal(e) if busyGroup(e) => () }
    })

    created.map { _ =>
      val start   = GroupCursor(pending = named.map(_ => names.map(_ -> "0-0").toMap), claims = Map.empty, lastClaim = 0L)
      val read    = Source.unfoldAsync(start)(step).mapConcat(identity)
      // at most once: acked as it leaves, what was read and is still here when the subscription ends stays
      // pending, for another member
      val entries =
        if (acks) read
        else read.map { received =>
          received.ack().failed.foreach(e => logger.warn(s"could not ack an entry: ${ConnectionManager.describe(e)}"))
          ReceivedEvent.unacked(received.message)
        }
      Subscribed(entries) { () =>
        reader.closeAsync().asScala.flatMap { _ =>
          // a member gone with nothing pending is forgotten. with entries pending, it stays until they are
          // claimed by another one; a named consumer stays, it comes back for them
          if (named.isDefined) Future.unit
          else
            Future
              .sequence(names.map(stream => shared.xpending(stream, group).asScala.map(p => Option(p.getConsumerMessageCount.get(consumer.getName)).map(_.longValue()).getOrElse(0L))))
              .flatMap { counts =>
                if (counts.forall(_ == 0L)) Future.sequence(names.map(stream => shared.xgroupDelconsumer(stream, consumer).asScala)).map(_ => ())
                else Future.unit
              }
        }.recover { case NonFatal(_) => () }
        ()
      }
    }
  }
}
