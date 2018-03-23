package io.cimpress.mcp.streams.sqs

import org.scalatest._

abstract class UnitSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors with BeforeAndAfterAll