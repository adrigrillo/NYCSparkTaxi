# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
Stablishes the global variables used in this project

This file contains data which is inmutable used for solving the
problem, like the size of the map and the cells.
"""
from datetime import datetime


LATITUDE = 0.004491556
LONGITUDE = 0.005986
MAPSIZE = 300
INITIAL_LATITUDE = 41.477182778
INITIAL_LONGITUDE = -74.916578
FINAL_LATITUDE = 40.129715978
FINAL_LONGITUDE = -73.120778


def obtener_timestamp(fecha, hora):
    """
    Metodo para obtener un timestamp con una fecha y una hora
    :param fecha: Fecha deseada en formato YY-MM-DD
    :param hora: Hora deseada en formato HH:MM
    :return: timestamp con la fecha y hora introducidos
    """
    return datetime.strptime(fecha + " " + hora + ":00", "%Y-%m-%d %H:%M:%S")


def obtener_dia_semana(dia):
    """
    Metodo para obtener el dia de la semana en formato int para
    poder utilizarlo con los datos del sistema. Donde 0 es el
    lunes y el 6 el domingo.
    :param dia: dia de la semana escrito en un string
    :return: dia de la semana en formato int
    """
    dia = dia.lower()
    numero_dia = 0
    if dia == "lunes":
        numero_dia = 0
    elif dia == "martes":
        numero_dia = 1
    elif dia == "miercoles" or dia == "miércoles":
        numero_dia = 2
    elif dia == "jueves":
        numero_dia = 3
    elif dia == "viernes":
        numero_dia = 4
    elif dia == "sabado" or dia == "sábado":
        numero_dia = 5
    elif dia == "domingo":
        numero_dia = 6
    else:
        print("No has establecido un día de la semana, por defecto es el lunes")
        numero_dia = 0
    return numero_dia


def obtener_mes(mes):
    """
    Metodo para obtener el mes de la semana en formato int para
    poder utilizarlo con los datos del sistema. Donde 1 es enero
    y el 12 diciembre.
    :param dia: dia de la semana escrito en un string
    :return: dia de la semana en formato int
    """
    mes = mes.lower()
    numero_mes = "01"
    if mes == "enero":
        numero_mes = "01"
    elif mes == "febrero":
        numero_mes = "02"
    elif mes == "marzo":
        numero_mes = "03"
    elif mes == "abril":
        numero_mes = "04"
    elif mes == "mayo":
        numero_mes = "05"
    elif mes == "junio":
        numero_mes = "06"
    elif mes == "julio":
        numero_mes = "07"
    elif mes == "agosto":
        numero_mes = "08"
    elif mes == "septiembre":
        numero_mes = "09"
    elif mes == "octubre":
        numero_mes = "10"
    elif mes == "noviembre":
        numero_mes = "11"
    elif mes == "diciembre":
        numero_mes = "12"
    else:
        print("No has establecido un mes, por defecto es enero")
    return numero_mes
