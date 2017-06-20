# -*- coding: utf-8 -*-
#!/usr/bin/env python
## Imports
import timeit
import sys
from datetime import timedelta, datetime
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from settings import obtener_timestamp


## Constantes
APP_NAME = "Most frequent routes"


def main(spark, fichero, fecha, hora):
    """
    Calculo de las rutas mas frecuentes dada una fecha y hora
    dentro de todo el conjunto de datos.
    :param spark: Instancia de spark
    :param fichero: Fichero de datos
    :param fecha: String con la fecha de busqueda en forma "YYYY-MM-DD".
    Ej: "2013-01-02"
    :param hora: Hora sobre la que se quiere realizar la consulta en forma "HH:MM"
    :return: Diez rutas mas frecuentes
    """
    inicio = timeit.default_timer()
    data = spark.read.format("parquet").load("./../data/processed/" + fichero)
    tiempo_fin = obtener_timestamp(fecha, hora)
    tiempo_inicio = tiempo_fin - timedelta(minutes=30)
    frequent = data.filter(data.hora_subida <= tiempo_fin) \
        .filter(data.hora_subida >= tiempo_inicio) \
        .filter(data.hora_bajada <= tiempo_fin) \
        .filter(data.hora_bajada >= tiempo_inicio) \
        .groupBy("cuad_longitud_subida", "cuad_latitud_subida", \
        "cuad_longitud_bajada", "cuad_latitud_bajada") \
        .count().orderBy("count", ascending=False)
    frequent = frequent.take(10)
    fin = timeit.default_timer()
    file = open("./../data/results/lab/standalone/" + "resultadosFrequent.txt", "a")
    file.write(str(tiempo_inicio) + ", " + str(tiempo_fin) + ", ")
    for i in range(len(frequent)):
        file.write(str(i) + ": ")
        file.write("(" + str(frequent[i][0]) + ", " + str(frequent[i][1]) + ") ")
        file.write("(" + str(frequent[i][2]) + ", " + str(frequent[i][3]) + "), ")
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
