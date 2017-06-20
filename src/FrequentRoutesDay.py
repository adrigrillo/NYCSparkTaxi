# -*- coding: utf-8 -*-
#!/usr/bin/env python
## Imports
import timeit
import sys
from datetime import timedelta, datetime
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, udf
from pyspark.sql.types import BooleanType, IntegerType
from settings import obtener_dia_semana, obtener_mes


## Constantes
APP_NAME = "Most frequent routes given a day"


## Variables globales
FICHERO = sys.argv[1]
MES = obtener_mes(sys.argv[2])
DIA_SEMANA = sys.argv[3]
HORA = sys.argv[4]
HORA_FIN = datetime.strptime("2013-" + MES + " " + HORA, "%Y-%m %H:%M")
HORA_INICIO = HORA_FIN - timedelta(minutes=30)


def comparar_hora(hora):
    """
        Metodo que filtra las horas de los registros para que concuerden
        con las horas de busqueda deseada
        :param hora: Timestamp completo
        :return: True si las horas del timestamp estan entre las deseadas
        False si lo contrario
    """
    if hora.time() <= HORA_FIN.time() and hora.time() >= HORA_INICIO.time():
        return True
    return False


def relevancia(fecha):
    """
        Metodo que da mas relevancia a los viajes mas cercanos a la
        fecha de busqueda deseada.
        Si la diferencia es menor a un mes de la fecha
        dada los registros tienen más relevancia
        :param fecha: Timestamp completo
        :return: 2 si el viaje esta cerca de la fecha deseada, 1 si no
    """
    diferencia = fecha - HORA_FIN
    if diferencia < timedelta(days=30) and diferencia > timedelta(days=-30):
        return 2
    else:
        return 1


comprobar_hora = udf(comparar_hora, BooleanType())
calcular_relevancia = udf(relevancia, IntegerType())


def main(spark, fichero):
    """
    Calculo de las rutas mas frecuentes dado un mes, un dia de la semana y
    una hora dentro de todo el conjunto de datos. Los viajes mas cercanos
    al mes introducido tendran mas relevancia
    :param spark: Instancia de spark
    :param fichero: Fichero de datos
    :return: Diez rutas mas frecuentes
    """
    inicio = timeit.default_timer()

    data = spark.read.format("parquet").load("./../data/processed/" + fichero)

    dia_elegido = obtener_dia_semana(DIA_SEMANA)

    """
    Filtramos los datos con respecto al dia de la semana y la hora
    Ademas le damos un relevancia a cada viaje para el posterior count
    """
    filtered = data.filter(data.dia_semana == dia_elegido) \
        .withColumn("joder", comprobar_hora(data.hora_subida)) \
        .withColumn("joder2", comprobar_hora(data.hora_bajada)) \
        .withColumn('relevancia', calcular_relevancia(data.hora_subida))
    """
    Agrupamos por rutas y hacemos el recuento de viajes
    """
    frequent = filtered.groupBy("cuad_longitud_subida", "cuad_latitud_subida", \
                                "cuad_longitud_bajada", "cuad_latitud_bajada") \
        .sum("relevancia") \
        .select(col("cuad_longitud_subida"), col("cuad_latitud_subida"), \
                col("cuad_longitud_bajada"), col("cuad_latitud_bajada"), \
                col("sum(relevancia)").alias("frecuencia")) \
    .orderBy("frecuencia", ascending=False)

    final = frequent.take(10)

    fin = timeit.default_timer()
    file = open("./../data/results/lab/standalone/" + "resultadosFrequentDay.txt", "a")
    file.write(str(HORA_INICIO.time()) + ", " + str(HORA_FIN.time()) + ", ")
    for i in range(len(final)):
        file.write(str(i) + ": ")
        file.write("(" + str(final[i][0]) + ", " + str(final[i][1]) + ") ")
        file.write("(" + str(final[i][2]) + ", " + str(final[i][3]) + "), ")
    file.write(str(fin - inicio) + "\n")
    file.close()


if __name__ == "__main__":
    # Configuramos SparkConf
    CONF = SparkConf()
    CONF.setAppName(APP_NAME)
    CONF.setMaster("local[*]")
    SPARK = SparkSession.builder.config(conf=CONF).getOrCreate()
    main(SPARK, FICHERO)
