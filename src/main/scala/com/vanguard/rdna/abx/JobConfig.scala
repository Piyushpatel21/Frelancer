package com.vanguard.rdna.abx

case class JobConfig(env: String, catalogId: String, _nodes: String) {
  def isProd: Boolean = "prod".equalsIgnoreCase(env)
  val nodes: String = String.valueOf(math.max((_nodes.toInt - 1) * 16, 200))
}