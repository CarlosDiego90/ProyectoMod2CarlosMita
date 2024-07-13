# -*- coding: utf-8 -*-
"""
Created on Fri Jul 12 21:24:58 2024

@author: CarlosMita
"""
#1. Conexión a la base de datos:
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import pandas as pd
import numpy as np

# Crear una clase base para el modelo declarativo
Base = declarative_base()

# Definir las clases para las tablas (como se proporcionó en tu código)
class CountryInfo(Base):
    __tablename__ = 'country_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    country_name = Column(String, nullable=False)
    country_code = Column(String, unique=True, nullable=False)
    region = Column(String, nullable=False)
    income_group = Column(String, nullable=False)
    num_ci = Column(Integer, nullable=False)  
    yearly_values = relationship("YearlyValue", back_populates="country_info")

class Indicator(Base):
    __tablename__ = 'indicator'
    id = Column(Integer, primary_key=True, autoincrement=True)
    indicator_name = Column(String, nullable=False)
    indicator_code = Column(String, unique=True, nullable=False)
    topic = Column(String, nullable=False)
    yearly_values = relationship("YearlyValue", back_populates="indicator")

class YearlyValue(Base):
    __tablename__ = 'yearly_value'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    value = Column(Float, nullable=False)
    country_info_id = Column(Integer, ForeignKey('country_info.id'), nullable=False)
    indicator_id = Column(Integer, ForeignKey('indicator.id'), nullable=False)
    country_info = relationship("CountryInfo", back_populates="yearly_values")
    indicator = relationship("Indicator", back_populates="yearly_values")

# Conexión a la base de datos
DATABASE_URL = "postgresql+psycopg2://utb_students:AVNS_OXQBajkVtAn2czuYQYe@pg-diplomado-utb-diplomado-utb-2024.c.aivencloud.com:24354/economic_kpis_utb"
engine = create_engine(DATABASE_URL)

# Crear una sesión
Session = sessionmaker(bind=engine)
session = Session()

# 2. Extracción de datos
# Función para obtener países asignados
def get_assigned_countries(num_ci):
    countries = session.query(CountryInfo).filter(CountryInfo.num_ci == num_ci).all()
    return [country.country_code for country in countries]

# Reemplazamos el número de carnet
carnet = 8303073
assigned_countries = get_assigned_countries(carnet)

# Obtener los IDs de los indicadores necesarios
indicator_codes = ['SI.POV.GINI', 'NY.GDP.PCAP.KD', 'SP.POP.TOTL', 'SI.POV.LMIC.GP']
indicators = session.query(Indicator).filter(Indicator.indicator_code.in_(indicator_codes)).all()
indicator_ids = {ind.indicator_code: ind.id for ind in indicators}

# Extraer datos para el cálculo del IPS
data = []
for country_code in assigned_countries:
    country_info = session.query(CountryInfo).filter(CountryInfo.country_code == country_code).first()
    
    # Obtener todos los años disponibles para este país
    available_years = session.query(YearlyValue.year).distinct().filter(
        YearlyValue.country_info_id == country_info.id
    ).order_by(YearlyValue.year).all()
    
    for (year,) in available_years:
        # Validamos si existen todos los datos necesarios para este año
        all_data_exists = all(
            session.query(YearlyValue).filter(
                YearlyValue.country_info_id == country_info.id,
                YearlyValue.indicator_id == indicator_id,
                YearlyValue.year == year
            ).first() is not None
            for indicator_id in indicator_ids.values()
        )
        
        # Si todos los datos existen, procedemos a extraerlos
        if all_data_exists:
            year_data = {'country_code': country_code, 'year': year}
            for indicator_code, indicator_id in indicator_ids.items():
                value = session.query(YearlyValue.value).filter(
                    YearlyValue.country_info_id == country_info.id,
                    YearlyValue.indicator_id == indicator_id,
                    YearlyValue.year == year
                ).scalar()
                year_data[indicator_code] = value
            data.append(year_data)

df = pd.DataFrame(data)

# 3. Calculo del IPS
def calculate_ips(row):
    Y = row['NY.GDP.PCAP.KD']
    P = row['SP.POP.TOTL']
    G = row['SI.POV.GINI']
    pov = row['SI.POV.LMIC.GP']
    return (Y / P) * (1 - G) * (1 - pov)

df['IPS'] = df.apply(calculate_ips, axis=1)

# CREAMOS EL DATA FRAME 'RESUMEN' PARA VISUALIZAR LOS DATOS OBTENIDOS

# Usamos el DataFrame 'df' con los datos del IPS

# Obtener los nombres de los países
country_names = {}
for country_code in df['country_code'].unique():
    country_info = session.query(CountryInfo).filter(CountryInfo.country_code == country_code).first()
    if country_info:
        country_names[country_code] = country_info.country_name

# Crear el DataFrame de resumen
resumen = pd.DataFrame({
    'CI': carnet,
    'PAIS': df['country_code'].map(country_names),
    'AÑO': df['year'],
    'IPS': np.round(df['IPS'], 5)
})

# Ordenar el DataFrame por nombre de país y año
resumen = resumen.sort_values(['PAIS', 'AÑO'])

# Restablecer el índice
resumen = resumen.reset_index(drop=True)

# Mostrar las primeras filas del DataFrame de resumen
print(resumen.head())


# 4. Subimos los datos de IPS a la Base de datos

# Obtener el ID del indicador IPS
ips_indicator = session.query(Indicator).filter(Indicator.indicator_code == 'SI.PROSP.IDX').first()
if not ips_indicator:
    ips_indicator = Indicator(indicator_name='Índice de Prosperidad Sostenible', indicator_code='SI.PROSP.IDX', topic='Sostenibilidad')
    session.add(ips_indicator)
    session.commit()

# Contador para registros insertados o actualizados
records_processed = 0

# Insertar los valores calculados
for _, row in df.iterrows():
    country_info = session.query(CountryInfo).filter(CountryInfo.country_code == row['country_code']).first()
    existing_value = session.query(YearlyValue).filter(
        YearlyValue.country_info_id == country_info.id,
        YearlyValue.indicator_id == ips_indicator.id,
        YearlyValue.year == row['year']
    ).first()

    if existing_value:
        existing_value.value = row['IPS']
    else:
        new_value = YearlyValue(
            year=row['year'],
            value=row['IPS'],
            country_info_id=country_info.id,
            indicator_id=ips_indicator.id
        )
        session.add(new_value)
    
    records_processed += 1

# Commit de los cambios
session.commit()

# VERIFICAMOS LA CARGA DE LOS DATOS EN LA BD
print(f"Se procesaron {records_processed} registros.")

# Verificamos algunos registros aleatoriamente.
sample_size = min(7, len(df))
sample_data = df.sample(n=sample_size)

for _, row in sample_data.iterrows():
    country_info = session.query(CountryInfo).filter(CountryInfo.country_code == row['country_code']).first()
    db_value = session.query(YearlyValue).filter(
        YearlyValue.country_info_id == country_info.id,
        YearlyValue.indicator_id == ips_indicator.id,
        YearlyValue.year == row['year']
    ).first()

    if db_value and abs(db_value.value - row['IPS']) < 0.0001:
        print(f"Verificado: País {row['country_code']}, Año {row['year']}, IPS {row['IPS']}")
    else:
        print(f"Error en la verificación: País {row['country_code']}, Año {row['year']}")

# Verificamos el total de registros en la base de datos para el indicador IPS
total_records = session.query(YearlyValue).filter(YearlyValue.indicator_id == ips_indicator.id).count()
print(f"Total de registros IPS en la base de datos: {total_records}")




