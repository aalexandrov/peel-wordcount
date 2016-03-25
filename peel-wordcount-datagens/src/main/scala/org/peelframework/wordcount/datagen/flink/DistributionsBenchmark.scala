package org.peelframework.wordcount.datagen.flink

import org.peelframework.wordcount.datagen.util.RanHash
import org.apache.commons.math3.distribution._

object DistributionsBenchmark {

  def main(args: Array[String]): Unit = {

    val C = 15
    val N = 100
    val M = 10000

    val u = new UniformIntegerDistribution(0, N - 1)
    val b = new BinomialDistribution(N - 1, 0.5)
    val n = new NormalDistribution(N/2.0, N/6.0)
    val z = new ZipfDistribution(N, 1)
    val p = new ParetoDistribution(N, 1)

    val r = new RanHash(0xC00FFEE)

    System.out.println(s"#" * C * 6)
    System.out.println(s"# Distributions inverse CDF benchmark")
    System.out.println(s"#" * C * 6)
    System.out.println(s"")

    System.out.print(s"Cardinality".padTo(C, ' '))
    System.out.print(s"Uniform".padTo(C, ' '))
    System.out.print(s"Binomial".padTo(C, ' '))
    System.out.print(s"Normal".padTo(C, ' '))
    System.out.print(s"Zipf".padTo(C, ' '))
    System.out.println(s"Pareto".padTo(C, ' '))
    System.out.println(s"-" * C * 6)

    for (N <- Seq(1000, 10000, 100000, 1000000, 10000000, 100000000)) {
      System.out.print(s"$N".padTo(C, ' '))

      val tu = time { for (_ <- 0 to M) u.inverseCumulativeProbability(r.next()) }
      System.out.print(s"${tu}ms".padTo(C, ' '))

      val tb = time { for (_ <- 0 to M) b.inverseCumulativeProbability(r.next()) }
      System.out.print(s"${tb}ms".padTo(C, ' '))

      val tn = time { for (_ <- 0 to M) n.inverseCumulativeProbability(r.next()) }
      System.out.print(s"${tn}ms".padTo(C, ' '))

      val tz = time { for (_ <- 0 to M) z.inverseCumulativeProbability(r.next()) }
      System.out.print(s"${tz}ms".padTo(C, ' '))

      val pz = time { for (_ <- 0 to M) p.inverseCumulativeProbability(r.next()) }
      System.out.println(s"${pz}ms".padTo(C, ' '))
    }
  }

  def time[R](block: => R, runs: Short = 9): Double = {
    val t = for (i <- (0 until runs).toList) yield {
      val t0 = System.nanoTime()
      block // call-by-name
      val t1 = System.nanoTime()
      t1 - t0
    }
    t.sorted.apply(runs/2)/1000.0
  }

}
