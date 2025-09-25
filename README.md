# Proyecto-Analisis-Opiniones

# Proyecto: Sistema de Análisis de Opiniones de Clientes

Este proyecto es una tarea para la materia Electiva 1.

## Descripción

El sistema implementa un pipeline ETL (Extract, Transform, Load) en Python para consolidar y procesar opiniones de clientes desde múltiples fuentes (archivos CSV).

### Funcionalidades
- Lee datos de productos, clientes y diversas fuentes de opinión.
- Limpia y procesa los datos para asegurar su integridad.
- Carga la información consolidada en una base de datos SQL Server.

## Cómo Ejecutar el Script

1.  Asegurarse de tener Python y las librerías necesarias (`pandas`, `sqlalchemy`, `pyodbc`).
2.  Configurar la cadena de conexión dentro del archivo `pipeline.py` con los datos del servidor SQL Server.
3.  Colocar todos los archivos `.csv` en la misma carpeta que el script.
4.  Ejecutar el script desde la terminal con el comando: `python pipeline.py`
