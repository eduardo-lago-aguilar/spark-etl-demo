package redbee

import java.util.Properties

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redbee.Settings._
import redbee.Settings.jdbc._

object Incremental extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("spark-etl-demo")
    .getOrCreate()

  import spark.implicits._

  //  benchmarks

  upserts

  def upserts = {
    val joined: RDD[(String, (Person, Option[Person]))] = doit("data/people_1K.csv")

    joined.foreach {
      case (id, (p0, Some(p1))) => println(s"update ${id} with ${p0} and ${p1}")
      case (id, (p0, None)) => println(s"insert ${id} with ${p0}")
    }
  }

  def benchmarks = {
    // warmup: looks like some previous warmup improves performance of subsequent benchmarks!
    doit("data/people_1K.csv")

    benchmark {
      doit("data/people_1K.csv")
    }
    benchmark {
      doit("data/people_4K.csv")
    }
    benchmark {
      doit("data/people_8K.csv")
    }
    benchmark {
      doit("data/people_16K.csv")
    }
    benchmark {
      doit("data/people_32K.csv")
    }
    benchmark {
      doit("data/people_128K.csv")
    }
    benchmark {
      doit("data/people_256K.csv")
    }
  }

  spark.stop()

  def doit(csvFile: String) = {

    // read and cache incremental CSV
    val csv: RDD[(String, Person)] = fromCsv(csvFile)

    // make a dynamic view from CSV ids
    // (SELECT * FROM person WHERE id IN ('1', '2', ..., '10000')) some_alias
    val view = makeView(csv, "person", "id")

    // read from postgresql using the dynamic view
    val pg: RDD[(String, Person)] = fromPg(view)

    // perform join
    val join: RDD[(String, (Person, Option[Person]))] = csv.leftOuterJoin(pg)

    // perform cleanup on RDD
    val clean: RDD[(String, Person)] = cleanup(join)

    print(s"Finished processing of ${csvFile}, with ${clean.count()} cleaned rows")

    join
  }

  def fromCsv(csv: String): RDD[(String, Person)] = {
    spark.read
      .option("header", true)
      .csv(csv)
      .as[Person]
      .map(p => p.id -> p)
      .rdd
      .partitionBy(new HashPartitioner(numPartitions))
      .cache()
  }

  def fromPg(query: String): RDD[(String, Person)] = {
    val jdbcUrl = s"jdbc:postgresql://${host}:${port}/${database}"
    val connectionProps: Properties = new Properties()
    connectionProps.put("user", user)
    connectionProps.put("password", password)

    spark.read
      .jdbc(url = jdbcUrl, table = query, properties = connectionProps)
      .as[Person]
      .map(p => p.id -> p)
      .rdd
  }

  def makeView(people: RDD[(String, Person)], table: String, keyField: String): String = {
    val values = people.map { case (id, _) => s"'${id}'" }.reduce((total, current) => s"${total}, ${current}")
    val query = s"select * from ${table} where ${keyField} in (${values}"
    s"(${query})) some_alias_for_${table}"
  }

  // TODO: silly implementation just to show up!
  def cleanup(join: RDD[(String, (Person, Option[Person]))]): RDD[(String, Person)] = join.mapValues { case (p0, _) => p0 }

  def benchmark[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println(" elapsed time: " + (t1 - t0) / 1000 / 1000 / 1000.0 + "s")
    result
  }


}
