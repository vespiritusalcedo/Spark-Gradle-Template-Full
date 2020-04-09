package template.spark

import org.apache.spark.sql.functions._

final case class Person(firstName: String, lastName: String,
                        country: String, age: Int)

object Main extends InitSpark {

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val sparkVersion = spark.version
    val scalaVersion = util.Properties.versionNumberString
    println("SPARK VERSION = " + sparkVersion)
    println("SCALA VERSION = " + scalaVersion)



    println("Reading from csv file: people-example.csv")
    val persons = reader.csv("people-example.csv").as[Person]
    persons.show(2)
    val averageAge = persons.agg(avg("age"))
                     .first.get(0).asInstanceOf[Double]
    println(f"Average Age: $averageAge%.2f")

    close
  }
}
