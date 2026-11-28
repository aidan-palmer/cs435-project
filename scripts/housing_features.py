"""Static housing attributes available before forecasting; excludes annual energy outputs."""
from pyspark.sql import functions as F


def join_housing(features,metadata):
    ids=features.select('building_id').distinct()
    raw=features.sparkSession.read.option('header',True).csv(str(metadata))
    homes=raw.where(F.col('upgrade')=='0').select(
        F.col('bldg_id').cast('string').alias('building_id'),
        F.col('`in.sqft`').cast('double').alias('housing_floor_area_sqft'),
        F.coalesce(F.col('`in.heating_fuel`'),F.lit('Unknown')).alias('housing_heating_fuel'))
    homes=homes.join(F.broadcast(ids),'building_id')
    if homes.groupBy('building_id').count().where('count != 1').limit(1).count():
        raise ValueError('Housing metadata must be unique per baseline building')
    if homes.count()!=ids.count():raise ValueError('Missing baseline housing metadata')
    value=F.col('housing_floor_area_sqft')
    if homes.where(value.isNull()|F.isnan(value)|(value<=0)|(value==float('inf'))).limit(1).count():
        raise ValueError('Invalid floor area in housing metadata')
    fuels=sorted(row[0] for row in homes.select('housing_heating_fuel').distinct().collect())
    columns=['housing_floor_area_sqft']
    for i,fuel in enumerate(fuels):
        column=f'housing_fuel_{i}'
        columns.append(column)
        homes=homes.withColumn(column,(F.col('housing_heating_fuel')==fuel).cast('double'))
    inventory=dict(source=str(metadata),fields=['in.sqft','in.heating_fuel'],feature_columns=columns,
                   fuel_vocabulary=fuels,method='Numeric floor area and deterministic one-hot fuel categories from static cohort metadata; no energy/outcome fields')
    return features.join(F.broadcast(homes.drop('housing_heating_fuel')),'building_id'),columns,inventory
