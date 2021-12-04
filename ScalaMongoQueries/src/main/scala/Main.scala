import org.bson.json.JsonWriterSettings
import org.mongodb.scala._
import org.mongodb.scala.model.Accumulators.sum
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters.{exists, _}
import org.mongodb.scala.model.Projections.{computed, excludeId, fields, include}
import org.mongodb.scala.model.Sorts.descending
import tour.Helpers._

object Main {

  /* Util */

  def prettify (document : Document) : String = document.toJson(JsonWriterSettings.builder().indent(true).build())

  /* Queries */

  def findCompanyByCeoName (collection : MongoCollection [Document],ceoNames: List[String]) : Document = {
    val companies = collection
      .find(in("ceo", ceoNames))
      .projection(fields(include("companyName", "ceo"), excludeId()))
      .first()
      .results()

    companies.size match {
      case 1 => companies.head
      case 0 => Document("Not found" -> "Not found")
    }
  }

  def topByMarketCapYearGt (collection: MongoCollection [Document], year : Int, limit : Int) : Seq [Document] = collection
    .find(gt("founded", year))
    .sort(descending("marketCap"))
    .limit(limit)
    .projection(fields(include("companyName", "marketCap", "founded"), excludeId()))
    .results()

  def companiesHavingMultipleHeadquarters (collection: MongoCollection[Document]) : Seq[Document] = collection
    .find(and(exists("headquarters", exists = true), where(s"this.headquarters.length>${1}")))
    .projection(fields(include("companyName", "headquarters"), excludeId()))
    .results()

  def companiesByNFiskalYearsProfitsGt(collection: MongoCollection[Document], fiskalYears: Int, value : Long) : Seq[Document] =
    collection.
    aggregate(List(
    project(
      fields(
        include("companyName"),
        excludeId(),
        Document("""{totalProfits: {$sum: {$slice: ["$fiskalGraph.profits", """ + fiskalYears + """]}}}""")
      )), filter(gt("totalProfits", value)))).results()

  def countriesByCompaniesProfits (collection: MongoCollection[Document], companiesLimit : Int) : Seq[Document] = collection
    .aggregate(List(
    project(
      fields(
        include("country"),
        excludeId(),
        Document("""{totalProfits: {$sum: "$fiskalGraph.profits"}}""")
      )),
    group("$country", sum("totalProfits", "$totalProfits")),
    sort(descending("totalProfits")),
    limit(companiesLimit),
    project(fields(
      computed("country", "$_id"),
      include("totalProfits"),
      excludeId()))))
    .results()

  def main(args: Array[String]): Unit = {
    System.setProperty("org.mongodb.async.type", "netty")

    val client: MongoClient = MongoClient()

    val db: MongoDatabase = client.getDatabase("Forbs")

    val companiesCollection : MongoCollection[Document] = db.getCollection("Companies")

    println("\nCompany with certain CEOs\n")

    val companyByCeoNames = findCompanyByCeoName(companiesCollection, List("Elon Musk"))
    println(prettify(companyByCeoNames))

    println("\nTop n companies by market cap founded later than *year*\n")

    val topByMarketCap = topByMarketCapYearGt(companiesCollection, 2000, 10)
    topByMarketCap.foreach(company => println(prettify(company)))

    println("\nCompanies that have multiple headquarters\n")

    val multipleHeadquarters = companiesHavingMultipleHeadquarters(companiesCollection)
    multipleHeadquarters.foreach(company => println(prettify(company)))

    println("\nCompanies by total profits profits in n fiskal years greater than m\n")

    val companiesByProfits = companiesByNFiskalYearsProfitsGt(companiesCollection, 5, 137500000000L)
    companiesByProfits.foreach(company => println(prettify(company)))

    println("\nTop countries by companies total profits\n")

    val topCountriesByCompaniesProfits = countriesByCompaniesProfits(companiesCollection, 5)
    topCountriesByCompaniesProfits.foreach(country => println(prettify(country)))

    client.close()
  }
}
