# Pipeline para DataSF (Fire Incidents Dataset)

Este pipeline rescata información desde DataSF, acerca de los incidentes de incendios (Fire Incidents). [Link a referencia del dataset](https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric).

Para ello, el presente proceso consume la API provista por DataSF mediante SodaPy (Python), para luego ingestar hacia PostgreSQL. El script generará las tablas y vistas desde cero en caso de que no existan. En caso contrario, solo será ingestada los datos faltantes, ello en función a la columna incident_date.

# Cómo ejecutar

Testeado funcionando en MacOS Sonoma 14.0 y openSUSE Tumbleweed. 
Se requiere Docker Desktop.

1. Clonar el repositorio

```
git clone https://github.com/deividxyz/fireincidents_pipeline fireincidents_pipeline
cd fireincidents_pipeline
```

2. Asignar variables de entorno en archivo .env

Debes crear un archivo .env, reemplazando las siguientes variables por los valores que correspondan:

```
APP_TOKEN=<token de la API de DataSF>
USERNAME=<usuario de DataSF (correo)>
PASSWORD=<contraseña de DataSF>
SCHEMA=<esquema de la base de datos de destino>
BATCH_SIZE=<tamaño del batch de datos a recuperar del api, debe ser entero>
POSTGRES_USER=<usuario de la base de datos de destino>
POSTGRES_PASSWORD=<contraseña de la base de datos de destino>
POSTGRES_DB=<nombre de la base de datos de destino>
```

Luego debes guardar el archivo .env en la carpeta fireincidents_pipeline.

3. Ejecutar

Debes correr el proyecto con Docker Compose, como se indica abajo:

```
docker compose up --build
```

Ello iniciará el contenedor propio de la ingesta y el de PostgreSQL. El puerto 5432 debe estar disponible para la base de datos.

# Artefactos generados

La ingesta copiará los datos raw hacia la tabla `fire_incidents` de la base de datos configurada en los parámetros anteriores.
También se generará una vista pensada para BI con nombre `vw_fire_incidents`.

Es posible conectarse a la instancia PostgreSQL, encontrándose disponible en `localhost` y en puerto `5432`. El usuario y contraseña deben ser los indicados en el archivo .env.