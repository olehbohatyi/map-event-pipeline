package com.github.olehbohatyi.mep.stores

import com.github.olehbohatyi.mep.domain.{ EventStatus, EventType, Severity }
import io.getquill.{ MappedEncoding, PostgresZioJdbcContext }

// Shared Quill context; enums are stored as their case names
object QuillContext extends PostgresZioJdbcContext(io.getquill.SnakeCase):

  given MappedEncoding[EventType, String]   = MappedEncoding(_.toString)
  given MappedEncoding[String, EventType]   = MappedEncoding(EventType.valueOf)
  given MappedEncoding[Severity, String]    = MappedEncoding(_.toString)
  given MappedEncoding[String, Severity]    = MappedEncoding(Severity.valueOf)
  given MappedEncoding[EventStatus, String] = MappedEncoding(_.toString)
  given MappedEncoding[String, EventStatus] = MappedEncoding(EventStatus.valueOf)
