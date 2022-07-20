package config

import io.prophecy.libs._
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

case class Config(fabricName: String, AI_MIN_DATETIME: String, AI_MAX_DATETIME: String) extends ConfigBase
