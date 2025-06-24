// 1. Initialisation et imports
import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Properties
import java.time.LocalDate
import java.time.temporal.ChronoUnit

// 2. Configuration de la session Spark
val spark = SparkSession.builder()
  .appName("AdventureWorks ETL")
  .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
  .config("spark.sql.shuffle.partitions", "8")
  .getOrCreate()

import spark.implicits._

// 3. Configuration des connexions aux bases de données
val config = Map(
  "mssql.jdbcUrl" -> "jdbc:sqlserver://host.docker.internal:1433;databaseName=AdventureWorks;encrypt=true;trustServerCertificate=true",
  "mssql.user" -> "SA",
  "mssql.password" -> "password123?",
  "postgres.jdbcUrl" -> "jdbc:postgresql://172.17.0.1:5432/adw_dw",
  "postgres.user" -> "postgres",
  "postgres.password" -> "postgres"
)

// 4. Fonction d'extraction
def extractTables(): Map[String, DataFrame] = {
  val jdbcProps = new Properties()
  jdbcProps.put("user", config("mssql.user"))
  jdbcProps.put("password", config("mssql.password"))
  jdbcProps.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

  val tables = Map(
    "person" -> "Person.Person",
    "customer" -> "Sales.Customer",
    "product" -> "Production.Product",
    "category" -> "Production.ProductCategory",
    "subcategory" -> "Production.ProductSubcategory",
    "order_header" -> "Sales.SalesOrderHeader",
    "order_detail" -> "Sales.SalesOrderDetail",
    "address" -> "Person.Address",
    "province" -> "Person.StateProvince",
    "country" -> "Person.CountryRegion"
  )

  tables.map { case (alias, tableName) =>
    val df = spark.read.jdbc(config("mssql.jdbcUrl"), tableName, jdbcProps)
    df.createOrReplaceTempView(alias)
    (alias, df)
  }
}

// 5. Nettoyage des données corrigé
def cleanData(dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
  dfs.map { case (name, df) =>
    val cleaned = name match {
      case "person" =>
        df.na.fill("Unknown", Seq("FirstName", "LastName"))
          .na.fill(0, Seq("EmailPromotion"))
          .dropDuplicates("BusinessEntityID")

      case "customer" =>
        df.na.drop(Seq("CustomerID", "PersonID"))
          .dropDuplicates("CustomerID")

      case "product" =>
        df.na.fill("N/A", Seq("Color", "Size"))
          .na.fill(0, Seq("StandardCost", "ListPrice"))
          .dropDuplicates("ProductID")

      case "order_header" =>
        df.na.drop(Seq("SalesOrderID", "CustomerID", "OrderDate"))
          .withColumn("OrderDate", to_date(col("OrderDate")))
          .dropDuplicates("SalesOrderID")

      case "order_detail" =>
        df.na.drop(Seq("SalesOrderID", "ProductID", "OrderQty"))
          .withColumn("UnitPrice", col("UnitPrice").cast(DecimalType(19, 4)))
          .withColumn("UnitPriceDiscount", col("UnitPriceDiscount").cast(DecimalType(19, 4)))
          .dropDuplicates(Seq("SalesOrderID", "SalesOrderDetailID"))

      case "address" =>
        // Suppression de "CountryRegionCode" car absente dans cette table
        df.na.drop(Seq("AddressID", "StateProvinceID"))
          .dropDuplicates("AddressID")

      case "province" =>
        df.na.drop(Seq("StateProvinceID", "CountryRegionCode"))
          .dropDuplicates("StateProvinceID")

      case "country" =>
        df.na.drop(Seq("CountryRegionCode"))
          .dropDuplicates("CountryRegionCode")

      case _ => df.dropDuplicates()
    }
    (name, cleaned)
  }
}

// 6. Construction des dimensions (avec cast explicite et jointures correctes)
def buildDimensions(dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
  val categoryRenamed = dfs("category").withColumnRenamed("Name", "CategoryName")
  val subcategoryRenamed = dfs("subcategory").withColumnRenamed("Name", "SubcategoryName")
  val provinceRenamed = dfs("province").withColumnRenamed("Name", "ProvinceName")
  val countryRenamed = dfs("country").withColumnRenamed("Name", "CountryName")

  val dimCustomer = dfs("customer")
    .join(dfs("person"), dfs("customer")("PersonID") === dfs("person")("BusinessEntityID"), "left")
    .select(
      col("CustomerID").as("CustomerKey"),
      coalesce(col("FirstName"), lit("Unknown")).as("FirstName"),
      coalesce(col("LastName"), lit("Unknown")).as("LastName"),
      col("EmailPromotion"),
      when(col("PersonType").isin("SC", "IN", "EM"), col("PersonType")).otherwise("UNK").as("CustomerType")
    )
    .distinct()

  val dimProduct = dfs("product")
    .join(subcategoryRenamed, Seq("ProductSubcategoryID"), "left")
    .join(categoryRenamed, Seq("ProductCategoryID"), "left")
    .select(
      col("ProductID").as("ProductKey"),
      col("Name").as("ProductName"),
      col("ProductNumber"),
      coalesce(col("CategoryName"), lit("Uncategorized")).as("ProductCategory"),
      coalesce(col("SubcategoryName"), lit("Uncategorized")).as("ProductSubcategory"),
      col("Color"),
      col("StandardCost"),
      col("ListPrice")
    )
    .distinct()

  val dimGeography = dfs("address")
    .join(provinceRenamed, Seq("StateProvinceID"), "left")
    .join(countryRenamed, Seq("CountryRegionCode"), "left")
    .select(
      col("AddressID").as("GeographyKey"),
      col("City"),
      col("ProvinceName").as("StateProvince"),
      col("CountryName").as("CountryRegion"),
      col("PostalCode")
    )
    .distinct()

  val minDate = dfs("order_header").select(min("OrderDate")).first().getDate(0).toLocalDate
  val maxDate = dfs("order_header").select(max("OrderDate")).first().getDate(0).toLocalDate
  val days = ChronoUnit.DAYS.between(minDate, maxDate).toInt

  val dimDate = spark.range(0, days + 1)
    .withColumnRenamed("id", "value")
    .withColumn("value", col("value").cast("int")) // cast nécessaire pour date_add
    .select(expr(s"date_add(to_date('$minDate'), value) as FullDate"))
    .select(
      date_format(col("FullDate"), "yyyyMMdd").cast("int").as("DateKey"),
      col("FullDate"),
      year(col("FullDate")).as("Year"),
      month(col("FullDate")).as("Month"),
      date_format(col("FullDate"), "MMMM").as("MonthName"),
      quarter(col("FullDate")).as("Quarter"),
      dayofweek(col("FullDate")).as("DayOfWeek"),
      date_format(col("FullDate"), "EEEE").as("DayName")
    )

  dimDate.createOrReplaceTempView("DimDate")

  Map(
    "DimCustomer" -> dimCustomer,
    "DimProduct" -> dimProduct,
    "DimGeography" -> dimGeography,
    "DimDate" -> dimDate
  )
}

// 7. Construction de la table de faits
def buildFactSales(dfs: Map[String, DataFrame]): DataFrame = {
  dfs("order_detail")
    .join(dfs("order_header"), Seq("SalesOrderID"), "inner")
    .withColumn("Revenue", col("OrderQty") * (col("UnitPrice") - col("UnitPriceDiscount")))
    .withColumn("DateKey", date_format(col("OrderDate"), "yyyyMMdd").cast("int"))
    .select(
      col("CustomerID").as("CustomerKey"),
      col("ProductID").as("ProductKey"),
      col("ShipToAddressID").as("GeographyKey"),
      col("DateKey"),
      col("OrderQty").as("OrderQuantity"),
      col("Revenue").as("SalesAmount"),
      col("UnitPrice"),
      col("SalesOrderID"),
      col("SalesOrderDetailID")
    )
}

// 8. Calcul des indicateurs métiers
def calculateBusinessMetrics(dimProduct: DataFrame, dimGeography: DataFrame, factSales: DataFrame): Map[String, DataFrame] = {
  val salesByCategory = factSales.join(dimProduct, Seq("ProductKey"))
    .groupBy("ProductCategory")
    .agg(
      sum("SalesAmount").as("TotalSales"),
      sum("OrderQuantity").as("TotalQuantity"),
      avg("UnitPrice").as("AveragePrice"),
      countDistinct("SalesOrderID").as("OrderCount")
    )
    .orderBy(desc("TotalSales"))

  val salesByRegion = factSales.join(dimGeography, Seq("GeographyKey"))
    .groupBy("CountryRegion", "StateProvince", "City")
    .agg(
      sum("SalesAmount").as("RegionalSales"),
      countDistinct("CustomerKey").as("CustomerCount")
    )
    .orderBy(desc("RegionalSales"))

  val salesTrend = factSales
    .join(dimProduct, Seq("ProductKey"))
    .join(dimGeography, Seq("GeographyKey"), "left")
    .join(spark.table("DimDate"), Seq("DateKey"), "left")
    .groupBy("Year", "Month", "MonthName", "ProductCategory")
    .agg(sum("SalesAmount").as("MonthlySales"))
    .orderBy("Year", "Month", "ProductCategory")

  Map(
    "SalesByCategory" -> salesByCategory,
    "SalesByRegion" -> salesByRegion,
    "SalesTrend" -> salesTrend
  )
}

// 9. Chargement dans PostgreSQL
def loadToPostgres(df: DataFrame, tableName: String): Unit = {
  val pgProps = new Properties()
  pgProps.put("user", config("postgres.user"))
  pgProps.put("password", config("postgres.password"))
  pgProps.put("driver", "org.postgresql.Driver")

  df.write.mode(SaveMode.Overwrite)
    .jdbc(config("postgres.jdbcUrl"), tableName, pgProps)
}

// 10. Exécution du pipeline ETL
println("=== DÉBUT DU PROCESSUS ETL ===")

println("\n1. Extraction des données depuis SQL Server...")
val sourceTables = extractTables()
println(s"✅ ${sourceTables.size} tables extraites")

println("\n2. Nettoyage des données...")
val cleanedTables = cleanData(sourceTables)
println("✅ Données nettoyées")

println("\n3. Construction des dimensions...")
val dimensions = buildDimensions(cleanedTables)
dimensions.foreach { case (name, df) =>
  println(s"✅ $name → ${df.count()} lignes")
}

println("\n4. Construction de la table de faits...")
val factSales = buildFactSales(cleanedTables)
println(s"✅ Table de faits construite → ${factSales.count()} lignes")

println("\n5. Calcul des indicateurs métiers...")
val businessMetrics = calculateBusinessMetrics(dimensions("DimProduct"), dimensions("DimGeography"), factSales)
businessMetrics.foreach { case (name, _) => println(s"✅ $name calculé") }

println("\n6. Chargement dans PostgreSQL...")
dimensions.foreach { case (name, df) => loadToPostgres(df, name) }
loadToPostgres(factSales, "FactCustomSales")
businessMetrics.foreach { case (name, df) => loadToPostgres(df, name) }
println("✅ Données chargées avec succès dans PostgreSQL")

println("\n=== APERÇU DES RÉSULTATS ===")
dimensions.foreach { case (name, df) =>
  println(s"\n★ $name:")
  df.show(5, truncate = false)
}
println("\n★ FactCustomSales:")
factSales.show(5, truncate = false)
businessMetrics.foreach { case (name, df) =>
  println(s"\n★ $name:")
  df.show(5, truncate = false)
}

println("\n=== PROCESSUS ETL TERMINÉ AVEC SUCCÈS ===")