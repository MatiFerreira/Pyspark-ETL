# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import expr, regexp_extract, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# COMMAND ----------

schema = StructType([
    StructField("COD_DISTRITO", IntegerType(), True),
    StructField("DESC_DISTRITO", StringType(), True),
    StructField("COD_DIST_BARRIO", IntegerType(), True),
    StructField("DESC_BARRIO", StringType(), True),
    StructField("COD_BARRIO", IntegerType(), True),
    StructField("COD_DIST_SECCION", IntegerType(), True),
    StructField("COD_SECCION", IntegerType(), True),
    StructField("COD_EDAD_INT", StringType(), True),
    StructField("ESPANOLESHOMBRES", IntegerType(), True),
    StructField("ESPANOLESMUJERES", IntegerType(), True),
    StructField("EXTRANJEROSHOMBRES", IntegerType(), True),
    StructField("EXTRANJEROSMUJERES", IntegerType(), True),
    StructField("FX_CARGA", TimestampType(), True),
    StructField("FX_DATOS_INI", StringType(), True),
    StructField("FX_DATOS_FIN", StringType(), True)
])
loadCSV = spark.read.csv('C:/Users/mathias.ferreira/Downloads/estadisticas202404.csv', sep=';', encoding='utf-8',
                         header=True, lineSep='\n', schema=schema,
                         ignoreTrailingWhiteSpace=True, ignoreLeadingWhiteSpace=True, emptyValue='0')
## SEGUNDA FORMA
laodcsv2 = (spark.read.format('csv').option('sep', ';').option('header', 'True').schema(schema)
            .load('C:/Users/mathias.ferreira/Downloads/estadisticas202404.csv')
            .show(5))

# COMMAND ----------

columns_to_trim = ["DESC_DISTRITO", "DESC_BARRIO", "COD_EDAD_INT", "FX_DATOS_INI", "FX_DATOS_FIN"]
for col_name in columns_to_trim:
    loadCSV = loadCSV.withColumn(col_name, trim(loadCSV[col_name]))

# COMMAND ----------

    # Aplicar la función trim a las columnas de tipo string
    loadCSV = (((loadCSV.withColumn("DESC_DISTRITO", trim(loadCSV["DESC_DISTRITO"])).withColumn("DESC_BARRIO", trim(
        loadCSV["DESC_BARRIO"]))
                 .withColumn("COD_EDAD_INT", trim(loadCSV["COD_EDAD_INT"])))
                .withColumn("FX_DATOS_INI", trim(loadCSV["FX_DATOS_INI"])))
               .withColumn("FX_DATOS_FIN", trim(loadCSV["FX_DATOS_FIN"])))

# COMMAND ----------

loadCSV.show()

# COMMAND ----------

distinct = loadCSV.distinct().take(1)

# COMMAND ----------

loadCSV.printSchema()

# COMMAND ----------

loadCSV.show()

# COMMAND ----------

# 6.3
# TEMPORAL VIEW
loadCSV.createOrReplaceTempView('vistaGlobalCSV')

# COMMAND ----------

spark.sql(" SELECT * FROM vistaGlobalCSV").show()

# COMMAND ----------

spark.table("vistaGlobalCSV")

# COMMAND ----------

## 6.4
## EJEMPLO DE SQL.
resultSQL = spark.sql("SELECT COUNT(DISTINCT(DESC_BARRIO)) FROM vistaGlobalCSV ").show()

# COMMAND ----------

loadCSV.select(countDistinct('DESC_BARRIO')).show()

# COMMAND ----------

# 6.5
newResult = spark.sql(" SELECT * ,LENGTH(DESC_BARRIO) AS LONGITUD FROM vistaGlobalCSV ")
newResult.show(5)

# COMMAND ----------

## OTRA FORMA DE HACER LO ANTERIOR:
otroResult = (loadCSV.select(col('*'), F.length(col('DESC_BARRIO')).alias('LONGITUD')))
otroResult.show(5)

# COMMAND ----------

# 6.6
colum5 = spark.sql(" SELECT * ,5 AS CONSTANTE  FROM vistaGlobalCSV").show()

# COMMAND ----------

##6.7 Eliminar la tabla SQL en Spark
spark.catalog.dropTempView("vistaGlobalCSV")


# COMMAND ----------

# 6.8
loadCSV.repartition('DESC_DISTRITO', 'DESC_BARRIO').cache()

# COMMAND ----------

# 6.10
result = spark.sql("SELECT DESC_DISTRITO, DESC_BARRIO, "
                   "SUM(CASE WHEN ESPANOLESHOMBRES IS NOT NULL THEN ESPANOLESHOMBRES ELSE 0 END) AS ESPANOLESHOMBRES, "
                   "SUM(CASE WHEN ESPANOLESMUJERES IS NOT NULL THEN ESPANOLESMUJERES ELSE 0 END) AS ESPANOLESMUJERES, "
                   "SUM(CASE WHEN EXTRANJEROSHOMBRES IS NOT NULL THEN EXTRANJEROSHOMBRES ELSE 0 END) AS EXTRANJEROSHOMBRES, "
                   "SUM(CASE WHEN EXTRANJEROSMUJERES IS NOT NULL THEN EXTRANJEROSMUJERES ELSE 0 END) AS EXTRANJEROSMUJERES "
                   "FROM vistaGlobalCSV "
                   "GROUP BY DESC_DISTRITO, DESC_BARRIO "
                   "ORDER BY EXTRANJEROSMUJERES DESC, EXTRANJEROSHOMBRES DESC"
                   ).show(5)


# COMMAND ----------

otherResult = (
    loadCSV.groupBy('DESC_DISTRITO', 'DESC_BARRIO')
    .agg(
        F.sum(F.when(col("ESPANOLESHOMBRES").isNull(), 0).otherwise(col('ESPANOLESHOMBRES'))).alias(
            'SUM_ESPANOLESHOMBRES'),
        F.sum(F.when(col("ESPANOLESMUJERES").isNull(), 0).otherwise(col('ESPANOLESMUJERES'))).alias(
            'SUM_ESPANOLESMUJERES'),
        F.sum(F.when(col("EXTRANJEROSHOMBRES").isNull(), 0).otherwise(col('EXTRANJEROSHOMBRES'))).alias(
            'SUM_EXTRANJEROSHOMBRES'),
        F.sum(F.when(col("EXTRANJEROSMUJERES").isNull(), 0).otherwise(col('EXTRANJEROSMUJERES'))).alias(
            'SUM_EXTRANJEROSMUJERES')
    ).orderBy(col('SUM_EXTRANJEROSMUJERES').desc()).orderBy(col('SUM_EXTRANJEROSHOMBRES').desc())
).show(5)

# COMMAND ----------

# 6.11
spark.catalog.clearCache()  # ELIMINA LOS VALORES EN CACHE.  

# COMMAND ----------

loadCSV = loadCSV.withColumn('ESPANOLESHOMBRES',
                             expr('CASE WHEN ESPANOLESHOMBRES IS NOT NULL THEN ESPANOLESHOMBRES ELSE 0 END'))
loadCSV.show()

# COMMAND ----------

totalDF = loadCSV.groupby('DESC_DISTRITO', 'DESC_BARRIO').agg(F.sum('ESPANOLESHOMBRES').alias('SUMA_TOTAL'))

# COMMAND ----------

newDF = totalDF.select('DESC_DISTRITO', 'DESC_BARRIO', 'SUMA_TOTAL')
newDF.show()

# COMMAND ----------

# 6.13
left = loadCSV.join(newDF, ['DESC_DISTRITO', 'DESC_BARRIO'], how='left')
left.show()

# COMMAND ----------

loadCSV = loadCSV.withColumn('COD_EDAD_INT', regexp_extract('COD_EDAD_INT', '(\d*)', 1))
loadCSV = loadCSV.withColumn('COD_EDAD_INT', loadCSV['COD_EDAD_INT'].cast(IntegerType()))

# COMMAND ----------

# 6.14
tabla_pivot = loadCSV.groupBy('COD_EDAD_INT').pivot('DESC_DISTRITO', ['BARAJAS', 'CENTRO', 'RETIRO'])   .agg(
    F.sum('ESPANOLESMUJERES')).orderBy('COD_EDAD_INT').sort('COD_EDAD_INT', ascending=False)
# Mostrar la tabla pivot
tabla_pivot.show()

# COMMAND ----------

porcent = loadCSV.groupBy('COD_EDAD_INT').pivot('DESC_DISTRITO', ['BARAJAS', 'CENTRO', 'RETIRO']).agg(
    F.round(F.mean('ESPANOLESMUJERES'), 2)).sort('COD_EDAD_INT', ascending=False)

porcent.show(50)

# COMMAND ----------

# 6.15
porcent = porcent.withColumn('sum_espanolesmujeres', F.round(F.col('BARAJAS') + F.col('CENTRO') + F.col('RETIRO'), 2))
# Calcula el porcentaje que representa cada distrito de la suma total
porcent = porcent.withColumn('%BARAJAS', F.round(F.col('BARAJAS') / F.col('sum_espanolesmujeres') * 100, 2))
porcent = porcent.withColumn('%CENTRO', F.round(F.col('CENTRO') / F.col('sum_espanolesmujeres') * 100, 2))
porcent = porcent.withColumn('%RETIRO', F.round(F.col('RETIRO') / F.col('sum_espanolesmujeres') * 100, 2))
# Muestra el DataFrame resultante
porcent.show(10)



# COMMAND ----------

## OTRA FORMA DE HACER LO DE ARRIBA PERO MAS OPTIMIZADO:
porcentNew = porcent.select('*',
                            F.round(F.col('BARAJAS') + F.col('CENTRO') + F.col('RETIRO'), 2).alias(
                                'sum_espanolesmujeres'),
                            F.round(col('BARAJAS') / col('sum_espanolesmujeres') * 100, 2).alias('%BARAJAS'),
                            F.round(col('CENTRO') / col('sum_espanolesmujeres') * 100, 2).alias('%CENTRO'),
                            F.round(col('RETIRO') / col('sum_espanolesmujeres') * 100, 2).alias('%RETIRO'),
                            )

porcentNew.show()

# COMMAND ----------

spark.stop()

# COMMAND ----------

# 6.16
# GUARDADO EN HADOOP
loadCSV.write.partitionBy('DESC_DISTRITO', 'DESC_BARRIO').csv(path='C:/Users/mathias.ferreira/Desktop/CSV',
                                                              mode='overwrite')
#GUARDADO EN LOCAL
loadCSV.write.csv(path='C:/Users/mathias.ferreira/Desktop/CSV', mode='overwrite')

# COMMAND ----------

# 6.17
# GUARDADO EN PARQUET
loadCSV.write.partitionBy('DESC_DISTRITO', 'DESC_BARRIO').format('parquet').mode("overwrite").save(
    'C:/Users/mathias.ferreira/Desktop')
