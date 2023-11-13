#!/usr/bin/env python3

import os
import pandas as pd

from sodapy import Socrata
from sqlalchemy import create_engine, text

appToken = os.getenv("APP_TOKEN")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
schema = os.getenv("SCHEMA")
batchSize = int(os.getenv("BATCH_SIZE"))

db_database = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")
db_host = "postgres"  # nombre del servicio en docker-compose.yml

print("")
print("Iniciando proceso de ingesta de datos...")
print("")
print("Username: {}".format(username))
print("Schema: {}".format(schema))
print("Batch Size: {}".format(batchSize))
print("")

def createEngine_Postgres():
    param_dic = {
        "host"      : db_host,
        "database"  : db_database,
        "user"      : db_user,
        "password"  : db_pass
    }
    connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
        param_dic['user'],
        param_dic['password'],
        param_dic['host'],
        param_dic['database']
    )
    engine = create_engine(connect)
    return engine

def createViewBD(eng):
    sqlView = """
    -- capa semántica para su uso en BI

    drop view if exists public.vw_fire_incidents;

    create or replace view public.vw_fire_incidents as
        select 
        cast(incident_number as bigint) as incident_number,
        cast(exposure_number as bigint) as exposure_number,
        cast(id as bigint) as id,
        address,  -- ya como text
        cast(incident_date as date) as incident_date,
        cast(call_number as bigint) as call_number,
        cast(alarm_dttm as timestamp) as alarm_dttm,
        cast(arrival_dttm as timestamp) as arrival_dttm,
        -- periodo de tiempo
        cast(arrival_dttm as timestamp) - cast(alarm_dttm as timestamp) as periodo_de_tiempo,  -- diferencia entre tiempo de llegada y alarma
        cast(close_dttm as timestamp) as close_dttm,
        city,
        zipcode,
        battalion as "batallón",  -- batallón
        station_area,
        box,
        cast(suppression_units as int) as suppression_units,
        cast(suppression_personnel as int) as suppression_personnel,
        cast(ems_units as int) as ems_units,
        cast(ems_personnel as int) as ems_personnel,
        cast(other_units as int) as other_units,
        cast(other_personnel as int) as other_personnel,
        cast(fire_fatalities as int) as fire_fatalities,
        cast(fire_injuries as int) as fire_injuries,
        cast(civilian_fatalities as int) as civilian_fatalities,
        cast(civilian_injuries as int) as civilian_injuries,
        cast(number_of_alarms as int) as number_of_alarms,
        primary_situation,
        mutual_aid,
        action_taken_primary,
        property_use,
        no_flame_spead,
        neighborhood_district as distrito,  -- distrito
        point,
        cast(supervisor_district as int) as supervisor_district,
        detector_alerted_occupants,
        area_of_fire_origin,
        ignition_cause,
        ignition_factor_primary,
        heat_source,
        item_first_ignited,
        human_factors_associated_with_ignition,
        estimated_contents_loss,
        action_taken_secondary,
        estimated_property_loss,
        structure_type,
        structure_status,
        floor_of_fire_origin,
        detectors_present,
        automatic_extinguishing_system_present,
        action_taken_other,
        automatic_extinguishing_sytem_type,
        automatic_extinguishing_sytem_perfomance,
        detector_type,
        detector_operation,
        number_of_sprinkler_heads_operating,
        detector_effectiveness,
        detector_failure_reason,
        automatic_extinguishing_sytem_failure_reason,
        number_of_floors_with_minimum_damage,
        fire_spread,
        ignition_factor_secondary
        from public.fire_incidents fi;
    """
    with eng.begin() as conn:
        conn.execute(text(sqlView))
    print("")
    print("Creada vista public.vw_fire_incidents ...")
    print("")

# conecto con la BD
eng = createEngine_Postgres()

client = Socrata("data.sfgov.org",
                 appToken,
                 username=username,
                 password=password)

fechaMinOrigin = client.get("wr8u-xric", select="distinct incident_date", order="incident_date ASC", limit=1)
fechaMaxOrigin = client.get("wr8u-xric", select="distinct incident_date", order="incident_date DESC", limit=1)

fechaMaxOriginStr = fechaMaxOrigin[0]['incident_date']
fechaMinOriginStr = fechaMinOrigin[0]['incident_date']

print("Fecha Minima Origen: {}".format(fechaMinOriginStr))
print("Fecha Maxima Origen: {}".format(fechaMaxOriginStr))
print("")

# evaluo si la tabla existe en la BD

sql = "select exists(select * from information_schema.tables where table_name='fire_incidents' and table_schema='{}')".format(schema)
tablaExiste = pd.read_sql(sql, eng)["exists"][0]

if tablaExiste:
    # leo la ultima fecha de carga existente en la BD
    maxFechaFireIncidentsDB = pd.read_sql("select max(incident_date) as maxfecha from {}.fire_incidents".format(schema), eng)
    maxFechaFireIncidentsDB = maxFechaFireIncidentsDB["maxfecha"][0]

    fechaMinDatasetStr = maxFechaFireIncidentsDB  # asigno la fecha minima del dataset a la ultima fecha de carga
    print("Fecha Maxima BD: {}".format(maxFechaFireIncidentsDB))
    print("")
    print("Tabla fire_incidents existe en la BD, se cargaran los datos desde {} hasta {}".format(fechaMinDatasetStr, fechaMaxOriginStr))

    # si la tabla existe, borro los registros de la ultima fecha de carga
    sql = "delete from {}.fire_incidents where incident_date >= '{}'".format(schema, maxFechaFireIncidentsDB)

    with eng.begin() as conn:
        conn.execute(text(sql))  # ejecuto el delete

else:
    fechaMinDatasetStr = fechaMinOriginStr

fechaMaxDatasetStr = fechaMaxOriginStr

# ajustes de paginación

currentOffset = 0

while True:
    # obtengo los datos de la API
    ingesta = client.get("wr8u-xric", limit=batchSize, offset=currentOffset, where="incident_date between '{}' and '{}'".format(fechaMinDatasetStr, fechaMaxDatasetStr))

    if len(ingesta) > 0:
        # transformo la lista de diccionarios en un dataframe
        ingesta_df = pd.DataFrame.from_records(ingesta)

        # convierto la columna point a un string, se que debería ser un Point pero tal parece que el dataset tiene datos erroneos
        ingesta_df["point"] = ingesta_df["point"].astype(str)

        if tablaExiste:
            ingesta_df.to_sql('fire_incidents', eng, schema=schema, if_exists="append")
        else:
            ingesta_df.to_sql('fire_incidents', eng, schema=schema, if_exists="replace")

        # actualizo el offset para cambiar de pagina
        currentOffset += batchSize

    else:
        createViewBD(eng)
        break

print("")
print("Ingesta actualizada. La BD sigue corriendo para que puedas revisar la data (Ctrl+C para cerrar) ...")
print("")

eng.dispose()
