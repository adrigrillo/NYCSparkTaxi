# ARQUITECTURA BIG DATA BASADA EN SPARK PARA ANALIZAR LOS VIAJES DE TAXI DE LA CIUDAD DE NUEVA YORK

Este repositorio contiene el código elaborado para el diseño, desarollo e implementación de una arquitectura big data basada en Apache Spark utizada para analizar los viajes de los taxis en la ciudad de Nueva York. Basado en el desafío presentado durante el DEBS Grand Challenge de 2015, el sistema realizará dos tipos de búsquedas, las diez rutas más frecuentes y las diez zonas que más beneficios aportarían al taxista, ambas tomando una ventana de tiempo para su realización.

La arquitectura estará basada principalmente en Apache Spark en todas sus implementaciones, además se usará Apacha Hadoop para la replicación de los datos en alguna de ellas. Apache Parquet será utilizado como formato por el sistema para guardar los datos procesados y para el análisis en las consultas.

La memoria de este TFG se encuentra disponible en la carpeta docs en formato Latex compilable.

# SPARK BASED BIG DATA ARCHITECTURE TO ANALYSE NEW YORK CITY TAXI TRIPS
In this Final Degree Project, we are going to take as starting point the challenge gave during the 2015 DEBS Grand Challenge. We are going to work with all the traces of the trips made during 2013 by the New York City taxis, which were more than 170 million of trips, and we will do some queries in them, measuring the efficiency of the systems used. There will be two queries, one that search the top ten frequent routes and the top ten profitable areas for the taxi driver, both of them taking in consideration a time window to do it.

This architecture will be based and developed in Apache Spark in all of its implementations and the distributed file system of Apache Hadoop in some. Apache Parquet will be used for the files to save the processed data as a file type.

The documentation of this final degree project could be found in docs folder in a compilable Latex format.