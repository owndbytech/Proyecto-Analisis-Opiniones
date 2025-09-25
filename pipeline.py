import pandas as pd
from sqlalchemy import create_engine, text

# --- 1. CONFIGURACIÓN DE LA CONEXIÓN ---
server = 'TAVAREZ' 
database = 'OpinionesETL' 
driver = 'ODBC Driver 17 for SQL Server' 
connection_string = f"mssql+pyodbc://{server}/{database}?driver={driver}&Trusted_Connection=yes"
engine = create_engine(connection_string)

print("🚀 Iniciando el pipeline ETL definitivo...")

# --- 2. LIMPIEZA TOTAL DE TABLAS ---
print("\n🧹 Vaciando todas las tablas para una carga limpia...")
try:
    with engine.connect() as connection:
        transaction = connection.begin()
        connection.execute(text("DELETE FROM Opiniones"))
        connection.execute(text("DELETE FROM Fuentes"))
        connection.execute(text("DELETE FROM Productos"))
        connection.execute(text("DELETE FROM Clientes"))
        transaction.commit()
    print("✅ Tablas vaciadas correctamente.")
except Exception as e:
    print(f"⚠️  No se pudieron vaciar las tablas. Error: {e}")

# --- 3. CARGA DE TABLAS DE DIMENSIONES (DATOS MAESTROS) ---
try:
    print("\n procesando fuentes...")
    df_fuentes = pd.read_csv('fuente_datos.csv')
    df_fuentes.rename(columns={'IdFuente': 'FuenteID', 'TipoFuente': 'Nombre', 'FechaCarga': 'FechaCarga'}, inplace=True)
    df_fuentes.to_sql('Fuentes', engine, if_exists='append', index=False)
    print("✅ Tabla 'Fuentes' cargada.")

    print("\n procesando productos...")
    df_productos = pd.read_csv('products.csv') 
    df_productos.rename(columns={'IdProducto': 'ProductoID', 'Nombre': 'Nombre', 'Categoría': 'Categoria'}, inplace=True)
    df_productos.to_sql('Productos', engine, if_exists='append', index=False)
    print("✅ Tabla 'Productos' cargada.")

    print("\n procesando clientes...")
    df_clientes = pd.read_csv('clients.csv')
    df_clientes.rename(columns={'IdCliente': 'ClienteID', 'NombreCompleto': 'Nombre', 'CorreoElectronico': 'Email'}, inplace=True)
    df_clientes.to_sql('Clientes', engine, if_exists='append', index=False)
    print("✅ Tabla 'Clientes' cargada.")

except Exception as e:
    print(f"❌ Error cargando las tablas de dimensiones: {e}")
    exit()

# --- 4. PROCESO ETL PARA LA TABLA DE OPINIONES ---
try:
    print("\n procesando y unificando todas las opiniones...")
    
    df_surveys = pd.read_csv('surveys_part1.csv')
    df_surveys.rename(columns={'IdCliente': 'ClienteID', 'IdProducto': 'ProductoID','PuntajeSatisfacción': 'Puntuacion', 'Comentario': 'Comentario', 'Fecha': 'Fecha'}, inplace=True)
    df_surveys['FuenteID'] = 'F002'

    df_web = pd.read_csv('web_reviews.csv')
    df_web.rename(columns={'IdCliente': 'ClienteID', 'IdProducto': 'ProductoID','Rating': 'Puntuacion', 'Comentario': 'Comentario', 'Fecha': 'Fecha'}, inplace=True)
    df_web['FuenteID'] = 'F001'

    df_social = pd.read_csv('social_comments.csv')
    df_social.rename(columns={'IdCliente': 'ClienteID', 'IdProducto': 'ProductoID','Comentario': 'Comentario', 'Fecha': 'Fecha'}, inplace=True)
    df_social['FuenteID'] = 'F005'

    df_consolidado = pd.concat([df_surveys, df_web, df_social], ignore_index=True)

    columnas_finales = ['ProductoID', 'ClienteID', 'Comentario', 'Puntuacion', 'Fecha', 'FuenteID']
    df_opiniones_final = df_consolidado[columnas_finales]
    
    print(f"📄 Se unificaron {len(df_opiniones_final)} opiniones en total (antes de limpiar).")

    # --- Filtro de Integridad Referencial (LA SOLUCIÓN FINAL) ---
    # Obtenemos una lista de los IDs que SÍ existen en nuestras tablas maestras.
    ids_productos_validos = pd.read_sql('SELECT ProductoID FROM Productos', engine)['ProductoID'].tolist()
    ids_clientes_validos = pd.read_sql('SELECT ClienteID FROM Clientes', engine)['ClienteID'].tolist()

    # Filtramos el dataframe de opiniones para quedarnos solo con las filas cuyos IDs son válidos.
    df_opiniones_validas = df_opiniones_final[
        df_opiniones_final['ProductoID'].isin(ids_productos_validos) &
        df_opiniones_final['ClienteID'].isin(ids_clientes_validos)
    ]
    
    print(f"🧼 Se encontraron {len(df_opiniones_validas)} opiniones con productos y clientes válidos.")

    # --- Limpieza Final ---
    df_opiniones_validas['Comentario'] = df_opiniones_validas['Comentario'].fillna('Sin comentario')
    df_opiniones_validas['Fecha'] = pd.to_datetime(df_opiniones_validas['Fecha'])
    df_opiniones_validas.drop_duplicates(inplace=True)
    
    print(f"📄 Se procesaron y unificaron {len(df_opiniones_validas)} opiniones listas para cargar.")

    # --- Cargar a la Base de Datos ---
    df_opiniones_validas.to_sql('Opiniones', engine, if_exists='append', index=False)
    print("📥 Todas las opiniones válidas fueron cargadas exitosamente en la tabla 'Opiniones'.")

except Exception as e:
    print(f"❌ Error procesando los archivos de opiniones: {e}")
    exit()

print("\n🎉 ¡PROCESO ETL COMPLETADO CON ÉXITO!")