# -*- coding: utf-8 -*-
#!/usr/bin/env python
## Imports
import sys
import timeit
from datetime import datetime
from decimal import *
from settings import *
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, \
DecimalType, TimestampType
from pyspark.sql.functions import floor, udf, abs

## Constantes
APP_NAME = "Data processing"


def dia_fecha(fecha):
    """
    Devuelve el dia de la semana en formato int
    Returns an int with the day of the weeek
    """
    return fecha.weekday()


CALCULAR_DIA = udf(dia_fecha, IntegerType())


def main(spark, fichero, nombre, tiempos):
    """
    Metodo que hace el procesamiento de los datos
    :param spark: Instancia de spark
    :param fichero: Fichero de datos
    :param nombre: Nombre del archivo resultante
    :param tiempos: Nombre del fichero de tiempos
    """
    inicio = timeit.default_timer()
    # Esquema de columnas del csv que se recibe
    nombre_columnas = StructType([StructField("medallon", StringType(), True), \
        StructField("licencia", StringType(), True), \
        StructField("hora_subida", TimestampType(), True), \
        StructField("hora_bajada", TimestampType(), True), \
        StructField("duracion_viaje_seg", IntegerType(), True), \
        StructField("distancia_viaje", DecimalType(precision=10, scale=2), True), \
        StructField("longitud_subida", DecimalType(precision=18, scale=14), True), \
        StructField("latitud_subida", DecimalType(precision=18, scale=14), True), \
        StructField("longitud_bajada", DecimalType(precision=18, scale=14), True), \
        StructField("latitud_bajada", DecimalType(precision=18, scale=14), True), \
        StructField("tipo_pago", StringType(), True), \
        StructField("tarifa", DecimalType(precision=10, scale=2), True), \
        StructField("recargo", DecimalType(precision=10, scale=2), True), \
        StructField("tasas", DecimalType(precision=10, scale=2), True), \
        StructField("propina", DecimalType(precision=10, scale=2), True), \
        StructField("peaje", DecimalType(precision=10, scale=2), True), \
        StructField("cantidad_total", DecimalType(precision=10, scale=2), True)])
    data = spark.read.csv("./../data/unprocessed/" + fichero, schema=nombre_columnas, \
        timestampFormat="yyyy-MM-dd HH:mm:ss")
    data.createOrReplaceTempView("unprocessed")
    # Filtrado de datos para eliminar registros incorrectos
    data = spark.sql("SELECT medallon, licencia, hora_subida, hora_bajada, duracion_viaje_seg, " \
        + "longitud_subida, latitud_subida, longitud_bajada, latitud_bajada, tipo_pago, tarifa, " \
        + "propina, cantidad_total " \
        + "FROM unprocessed " \
        + "WHERE medallon <> '' AND licencia <> '' AND hora_subida <> hora_bajada " \
        + "AND duracion_viaje_seg > 0 AND cantidad_total > 0 " \
        + "AND longitud_subida <> longitud_bajada AND latitud_subida <> latitud_bajada " \
        + "AND (tipo_pago = 'CSH' OR tipo_pago = 'CRD')")
    # Filtramos las latitudes para eliminar los registros invalidos
    data = data.where(data.longitud_subida >= INITIAL_LONGITUDE) \
        .where(data.longitud_subida <= FINAL_LONGITUDE) \
        .where(data.longitud_bajada >= INITIAL_LONGITUDE) \
        .where(data.longitud_bajada <= FINAL_LONGITUDE) \
        .where(data.latitud_subida >= FINAL_LATITUDE) \
        .where(data.latitud_subida <= INITIAL_LATITUDE) \
        .where(data.latitud_bajada >= FINAL_LATITUDE) \
        .where(data.latitud_bajada <= INITIAL_LATITUDE)
    # Establecemos el sistema de cuadriculas y calculamos el dia de la semana
    data = data.withColumn("cuad_latitud_subida", \
        floor((INITIAL_LATITUDE - data.latitud_subida)/LATITUDE) + 1) \
        .withColumn("cuad_longitud_subida", \
        floor(abs(INITIAL_LONGITUDE - data.longitud_subida)/LONGITUDE) + 1) \
        .withColumn("cuad_latitud_bajada", \
        floor((INITIAL_LATITUDE - data.latitud_bajada)/LATITUDE) + 1) \
        .withColumn("cuad_longitud_bajada", \
        floor(abs(INITIAL_LONGITUDE - data.longitud_bajada)/LONGITUDE) + 1) \
        .withColumn("dia_semana", CALCULAR_DIA(data.hora_subida))
    data.write.option("compression", "snappy") \
        .parquet("./../data/processed/" + nombre + "lab.parquet")
    fin = timeit.default_timer()
    file = open("./../data/results/" + tiempos + ".txt", "a")
    file.write(str(fin - inicio) + "\n")
    file.close()


if __name__ == "__main__":
    # Configuramos SparkConf
    CONF = SparkConf()
    CONF.setAppName(APP_NAME)
    CONF.setMaster("local[*]")
    SPARK = SparkSession.builder.config(conf=CONF).getOrCreate()
    FICHERO = sys.argv[1]
    NOMBRE = sys.argv[2]
    TIEMPOS = sys.argv[3]
    main(SPARK, FICHERO, NOMBRE, TIEMPOS)
