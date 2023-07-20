import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import java.nio.file.{Paths, Files}

val csvPath = "file:///Users/simon/Downloads/задача_кандидата/res.csv"

def loadLevel(level: Int) : Dataset[Row] = {
    val source = spark.read
        .options(Map("delimiter"->";","header"->"true"))
        .csv("задача_кандидата/admin_lev"+level+".txt")
    // source.show
    return source
        .select(
            col("OSM_ID"), 
            coalesce(col("ADMIN_L10D"),col("ADMIN_L9D"),col("ADMIN_L8D"),col("ADMIN_L6D"),col("ADMIN_L5D"),col("ADMIN_L4D"),col("ADMIN_L3D"),col("ADMIN_L2D")).as("PAR_OSM_ID"), 
            col("NAME")
        )
        .withColumn("LEVEL", lit(level))
        .withColumn("VALID_FROM", current_date())
        .withColumn("VALID_TO", lit("31-12-9999"))
}

var result = loadLevel(2)

val p = Array(3,4,5,6,8,9)
for(i <- p){
    result = result.union(loadLevel(i))
}

if (Files.exists(Paths.get("/Users/simon/Downloads/задача_кандидата/res.csv"))) {
    val snp = spark.read
        .format("csv")
        .option("header",true)
        .option("sep",";")
        .csv(csvPath)
    
    val snpOpen = snp
        .filter(col("VALID_TO").equalTo("31-12-9999"))

    val snpClosed = snp
        .filter(col("VALID_TO").notEqual("31-12-9999"))
    
    val newPart = result.join(snpOpen, result.col("OSM_ID").equalTo(snpOpen.col("OSM_ID")), "full")

    val closed = newPart.filter(result.col("OSM_ID").isNull)
        .select(
            snpOpen.col("OSM_ID"),
            snpOpen.col("PAR_OSM_ID"),
            snpOpen.col("NAME"),
            snpOpen.col("LEVEL"),
            snpOpen.col("VALID_FROM")
        )
        .withColumn("VALID_TO", current_date())
    
    val notChangedOrUpdated = newPart.filter(result.col("OSM_ID").isNotNull.and(snpOpen.col("OSM_ID").isNotNull))

    val notChanged = notChangedOrUpdated
        .filter(result.col("NAME").equalTo(snpOpen.col("NAME")).and(result.col("PAR_OSM_ID").equalTo(snpOpen.col("PAR_OSM_ID"))))
        .select(
            snpOpen.col("OSM_ID"),
            snpOpen.col("PAR_OSM_ID"),
            snpOpen.col("NAME"),
            snpOpen.col("LEVEL"),
            snpOpen.col("VALID_FROM"),
            snpOpen.col("VALID_TO")
        )
    
    val updated = notChangedOrUpdated
        .filter(
            result.col("NAME").notEqual(snpOpen.col("NAME"))
            .or(result.col("PAR_OSM_ID").notEqual(snpOpen.col("PAR_OSM_ID")))
        )

    val forClose = updated
        .select(
            snpOpen.col("OSM_ID"),
            snpOpen.col("PAR_OSM_ID"),
            snpOpen.col("NAME"),
            snpOpen.col("LEVEL"),
            snpOpen.col("VALID_FROM"),
        )
        .withColumn("VALID_TO", date_sub(current_date(), 1))

    val forOpen = updated
        .select(
            result.col("OSM_ID"),
            result.col("PAR_OSM_ID"),
            result.col("NAME"),
            result.col("LEVEL"),
            result.col("VALID_TO"),
        )
        .withColumn("VALID_FROM", current_date())

    val news = newPart.filter(snpOpen.col("OSM_ID").isNull)
        .select(
            result.col("OSM_ID"),
            result.col("PAR_OSM_ID"),
            result.col("NAME"),
            result.col("LEVEL"),
            result.col("VALID_FROM"),
            result.col("VALID_TO")
        )
    result = news.union(forOpen).union(forClose).union(notChanged).union(closed).union(snpClosed)
}

result.repartition(1).write
    .format("csv")
    .option("header",true)
    .mode("overwrite")
    .option("sep",";")
    .save(csvPath)