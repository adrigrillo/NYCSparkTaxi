# -*- coding: utf-8 -*-
#!/usr/bin/env python
## Imports
import timeit
import sys
from datetime import timedelta, datetime
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from settings import obtener_timestamp


## Constantes
APP_NAME = "Most profitable areas"


def main(spark, fichero, fecha, hora):
    """

    """
    inicio = timeit.default_timer()

    data = spark.read.format("parquet").load("./../data/processed/" + fichero)

    """ Establecemos los tiempos.
        - 30 minutos para los taxis
        - 15 minutos para los beneficios """
    tiempo = obtener_timestamp(fecha, hora)
    tiempo30 = tiempo - timedelta(minutes=30)
    tiempo15 = tiempo - timedelta(minutes=15)

    # Acotamos los viajes
    tripsDown = data.where(data.hora_bajada <= tiempo).where(data.hora_bajada > tiempo30)
    tripsUp = data.where(data.hora_subida <= tiempo).where(data.hora_subida > tiempo30)
    trips15 = tripsDown.where(data.hora_bajada <= tiempo).where(data.hora_bajada > tiempo15)

    """ PARTE PARA SACAR LOS TAXIS LIBRES """
    # Obtenemos los ultimos viajes de subida y bajada de los taxis
    bajadas30 = tripsDown \
        .select("medallon", "hora_bajada", "cuad_latitud_bajada", "cuad_longitud_bajada") \
        .orderBy("hora_bajada", ascending=False) \
        .dropDuplicates(subset=["medallon"])

    subidas30 = tripsUp \
        .select("medallon", "hora_subida").orderBy("hora_subida", ascending=False) \
        .dropDuplicates(subset=["medallon"]).withColumnRenamed("medallon", "taxi")

    """ Procedemos a juntar ambos conjuntos de datos para asi poder
        comprobar que no se tienen en cuenta los taxis que hayan sido
        cogidos de nuevo """
    # Spark falla join si el nombre de la columna es el mismo, renombrado a taxi
    joined = bajadas30.join(subidas30, bajadas30.medallon == subidas30.taxi, "leftouter") \
        .select("medallon", "hora_bajada", "hora_subida", \
        "cuad_latitud_bajada", "cuad_longitud_bajada")

    # Eliminamos los taxis que hayan sido cogidos tras la ultima bajada
    estado_taxis = joined \
        .select(joined.medallon, joined.cuad_latitud_bajada, joined.cuad_longitud_bajada, \
        when(joined.hora_subida > joined.hora_bajada, 1).otherwise(0).alias("taxi_ocupado"))
    taxis_filtrados = estado_taxis.filter(estado_taxis.taxi_ocupado == 0)
    taxis_libres = taxis_filtrados.groupBy("cuad_latitud_bajada", "cuad_longitud_bajada").count() \
        .select(col("cuad_latitud_bajada"), col("cuad_longitud_bajada"), \
                col("count").alias("taxis_libres"))

    """ PARTE PARA SACAR EL BENEFICIO DE UNA ZONA """
    beneficios = trips15.groupBy("cuad_latitud_subida", "cuad_longitud_subida") \
        .avg("tarifa", "propina") \
        .select(col("cuad_latitud_subida"), col("cuad_longitud_subida"), \
                (col("avg(tarifa)") + col("avg(propina)")).alias("beneficios"))

    """ UNION DE AMBAS TABLAS """
    condicion = [beneficios.cuad_latitud_subida == taxis_libres.cuad_latitud_bajada, \
        beneficios.cuad_longitud_subida == taxis_libres.cuad_longitud_bajada]

    profitable = beneficios.join(taxis_libres, condicion, "leftouter") \
        .select(col("cuad_latitud_subida").alias("cuad_latitud"), \
                col("cuad_longitud_subida").alias("cuad_longitud"), (col("beneficios") / \
            when(taxis_libres.taxis_libres > 0, (taxis_libres.taxis_libres + 1)).otherwise(1)).alias("beneficio")) \
        .orderBy("beneficio", ascending=False)

    profitable = profitable.take(10)
    fin = timeit.default_timer()
    file = open("./../data/results/lab/standalone/" + "resultadosProfitable.txt", "a")
    file.write(str(tiempo30) + ", " + str(tiempo) + ", ")
    for i in range(len(profitable)):
        file.write(str(i) + ": ")
        file.write("(" + str(profitable[i][0]) + ", " + str(profitable[i][1]) + ") ")
    file.write(str(fin - inicio) + "\n")
    file.close()


if __name__ == "__main__":
    # Configuramos SparkConf
    CONF = SparkConf()
    CONF.setAppName(APP_NAME)
    CONF.setMaster("local[*]")
    SPARK = SparkSession.builder.config(conf=CONF).getOrCreate()
    FICHERO = sys.argv[1]
    FECHA = sys.argv[2]
    HORA = sys.argv[3]
    main(SPARK, FICHERO, FECHA, HORA)
