# -*- coding: utf-8 -*-
#!/usr/bin/env python
## Imports
import timeit
import sys
from datetime import timedelta, datetime
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, udf, when, round
from pyspark.sql.types import BooleanType, FloatType
from settings import obtener_dia_semana, obtener_timestamp


## Constantes
APP_NAME = "Most profitable areas given a day"


## Variables globales
FICHERO = sys.argv[1]
DIA_SEMANA = sys.argv[2]
FECHA = sys.argv[3]
HORA = sys.argv[4]
HORA_FIN = obtener_timestamp("2013-" + FECHA, HORA)
HORA_30 = HORA_FIN - timedelta(minutes=30)
HORA_15 = HORA_FIN - timedelta(minutes=15)


def comparar_media_hora(hora):
    """
        Metodo que filtra las horas de los registros para que concuerden
        con las horas de busqueda deseada
        :param hora: Timestamp completo
        :return: True si las horas del timestamp estan entre las deseadas
        False si lo contrario
    """
    if hora.time() <= HORA_FIN.time() and hora.time() >= HORA_30.time():
        return True
    return False


def comparar_cuarto_hora(hora):
    """
        Metodo que filtra las horas de los registros para que concuerden
        con las horas de busqueda deseada
        :param hora: Timestamp completo
        :return: True si las horas del timestamp estan entre las deseadas
        False si lo contrario
    """
    if hora.time() <= HORA_FIN.time() and hora.time() >= HORA_15.time():
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
    if diferencia < timedelta(days=7) and diferencia > timedelta(days=-7):
        return 1.0
    elif diferencia < timedelta(days=14) and diferencia > timedelta(days=-14):
        return 0.75
    elif diferencia < timedelta(days=21) and diferencia > timedelta(days=-21):
        return 0.5
    elif diferencia < timedelta(days=-28) and diferencia > timedelta(days=-28):
        return 0.25
    else:
        return 0

comprobar_media_hora = udf(comparar_media_hora, BooleanType())
comprobar_cuarto_hora = udf(comparar_cuarto_hora, BooleanType())
calcular_relevancia = udf(relevancia, FloatType())


def main(spark, fichero):
    """
    Calculo de las zonas mas propensas a crear beneficio dado un mes,
    un dia de la semana y una hora dentro de todo el conjunto de datos.
    Los viajes mas cercanos al mes introducido tendran mas relevancia
    :param spark: Instancia de spark
    :param fichero: Fichero de datos
    :return: Diez rutas mas frecuentes
    """
    inicio = timeit.default_timer()

    data = spark.read.format("parquet").load("./../data/processed/" + fichero)

    dia_elegido = obtener_dia_semana(DIA_SEMANA)

    # Filtramos los datos con respecto al dia de la semana y la hora
    trips_down = data.filter(data.dia_semana == dia_elegido) \
        .filter(comprobar_media_hora(data.hora_bajada))

    trips_up = data.filter(data.dia_semana == dia_elegido) \
        .filter(comprobar_media_hora(data.hora_subida))

    # Borramos duplicados y los ordenamos por proximidad a la hora deseada
    down_30_min = trips_down.select("medallon", "hora_bajada", \
        "cuad_latitud_bajada", "cuad_longitud_bajada") \
        .orderBy("hora_bajada", ascending=False) \
        .dropDuplicates(subset=["medallon"])

    up_30_min = trips_up.select("medallon", "hora_subida").orderBy("hora_subida", ascending=False) \
    .dropDuplicates(subset=["medallon"]).withColumnRenamed("medallon", "taxi")

    #spark falla join si el nombre de la columna es el mismo, renombrado a taxi
    joined = down_30_min.join(up_30_min, down_30_min.medallon == up_30_min.taxi, "leftouter") \
        .select("medallon", "hora_bajada", "hora_subida", \
        "cuad_latitud_bajada", "cuad_longitud_bajada")

    # Sacamos los taxis que estan libres con su posicion
    estado_taxis = joined.select(joined.medallon, joined.cuad_latitud_bajada, \
        joined.cuad_longitud_bajada, joined.hora_bajada, \
        when(joined.hora_subida > joined.hora_bajada, 1).otherwise(0).alias("taxi_ocupado"))

    # Anyadimos el factor de influencia a cada taxi segun la distancia a fecha del registro
    taxis_filtrados = estado_taxis.filter(estado_taxis.taxi_ocupado == 0) \
        .withColumn("influencia", calcular_relevancia(estado_taxis.hora_bajada))

    # Calculamos el numero de taxis vacios en las zonas
    taxis_libres = taxis_filtrados.groupBy("cuad_latitud_bajada", "cuad_longitud_bajada").count() \
        .select(col("cuad_latitud_bajada"), col("cuad_longitud_bajada"), \
        col("count").alias("taxis_libres"))

    # Calculamos la influencia de la zona con la media de la influencia de los taxis en dicha zona
    influencia_taxis_libres = taxis_filtrados \
        .groupBy("cuad_latitud_bajada", "cuad_longitud_bajada") \
        .avg("influencia") \
        .select(col("cuad_latitud_bajada").alias("latitud"), \
                col("cuad_longitud_bajada").alias("longitud"), \
                col("avg(influencia)").alias("influencia"))

    """ Calculamos la proporcion de taxis libres por zona, esta proporcion
        es el numero de taxis libres en la zona por la influencia de estos taxis.
        Siendo menos influyentes cuanto mas alejados en el tiempo
    """
    condition = [taxis_libres.cuad_latitud_bajada == influencia_taxis_libres.latitud, \
        taxis_libres.cuad_longitud_bajada == influencia_taxis_libres.longitud]
    taxis_libres_prop = taxis_libres.join(influencia_taxis_libres, condition) \
        .select(col("cuad_latitud_bajada"), col("cuad_longitud_bajada"), \
                round(col("taxis_libres") * col("influencia")).alias("proporcion_taxis_libres"))

    """
        PASAMOS A LOS BENEFICIOS
    """
    # Filtramos viajes por tiempo y anyadimos influencia con respecto a la fecha deseada
    trips_15 = trips_down.filter(comprobar_cuarto_hora(data.hora_bajada)) \
        .withColumn("influencia", calcular_relevancia(estado_taxis.hora_bajada))

    # Obtenemos los beneficios de la zona con la influencia tenida en cuenta
    beneficios = trips_15.groupBy("cuad_latitud_subida", "cuad_longitud_subida") \
        .avg("tarifa", "propina", "influencia") \
        .select(col("cuad_latitud_subida"), col("cuad_longitud_subida"), \
                ((col("avg(tarifa)") + col("avg(propina)")) \
                * col("avg(influencia)")).alias("beneficios"))

    # Unimos los beneficios con los taxis libres y dividimos beneficios/taxis_libres
    condicion = [beneficios.cuad_latitud_subida == taxis_libres.cuad_latitud_bajada, \
                beneficios.cuad_longitud_subida == taxis_libres.cuad_longitud_bajada]
    profitable = beneficios.join(taxis_libres_prop, condicion, "leftouter") \
        .select(col("cuad_latitud_subida").alias("cuad_latitud"), \
                col("cuad_longitud_subida").alias("cuad_longitud"), \
                (col("beneficios") / col("proporcion_taxis_libres")).alias("beneficio")) \
        .orderBy("beneficio", ascending=False)

    # Obtenemos el top 10
    profitable = profitable.take(10)

    fin = timeit.default_timer()
    file = open("./../data/results/" + "resultadosProfitableDay.txt", "a")
    file.write(str(HORA_30) + ", " + str(HORA_FIN) + ", ")
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
    main(SPARK, FICHERO)
