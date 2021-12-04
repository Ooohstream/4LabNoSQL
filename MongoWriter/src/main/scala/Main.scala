import better.files._
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}

object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("org.mongodb.async.type", "netty")

    val client: MongoClient = MongoClient()

    val db: MongoDatabase = client.getDatabase("Forbs")

    val companiesCollection : MongoCollection[Document] = db.getCollection("Companies")

    val jsonDirectory : File = "C:/Users/OMEN/IdeaProjects/parser/jsonDocuments".toFile

    jsonDirectory.entries
      .foreach(file => companiesCollection.insertOne(Document(file.contentAsString))
      .subscribe(result => println(result.getInsertedId)))

    client.close()
  }
}
