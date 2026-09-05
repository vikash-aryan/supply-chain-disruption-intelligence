-- Supply Chain Disruption Intelligence - Table Schemas
-- Unity Catalog: workspace.supply_chain

-- Catalog and Schema
CREATE CATALOG IF NOT EXISTS workspace;
CREATE SCHEMA IF NOT EXISTS workspace.supply_chain;
USE CATALOG workspace;
USE SCHEMA supply_chain;

-- Bronze Layer: Synthetic AIS Data
CREATE TABLE IF NOT EXISTS ais_bronze (
  MMSI BIGINT COMMENT 'Maritime Mobile Service Identity',
  BaseDateTime STRING COMMENT 'Timestamp of AIS position',
  LAT DOUBLE COMMENT 'Latitude',
  LON DOUBLE COMMENT 'Longitude',
  SOG DOUBLE COMMENT 'Speed Over Ground (knots)',
  COG DOUBLE COMMENT 'Course Over Ground (degrees)',
  VesselType INT COMMENT 'Vessel type code',
  Status INT COMMENT 'Navigation status code',
  VesselName STRING COMMENT 'Vessel name',
  PortZone STRING COMMENT 'Port zone name'
) USING DELTA;

-- Silver Layer: Cleaned AIS Data  
CREATE TABLE IF NOT EXISTS ais_silver (
  MMSI BIGINT,
  BaseDateTime TIMESTAMP,
  LAT DOUBLE,
  LON DOUBLE,
  SOG DOUBLE,
  COG DOUBLE,
  VesselType INT,
  VesselTypeName STRING,
  Status INT,
  NavStatus STRING,
  VesselName STRING,
  PortZone STRING
) USING DELTA;

-- Reference: Port Zones
CREATE TABLE IF NOT EXISTS port_zones (
  port_zone STRING,
  center_lat DOUBLE,
  center_lon DOUBLE,
  lat_min DOUBLE,
  lat_max DOUBLE,
  lon_min DOUBLE,
  lon_max DOUBLE,
  description STRING
) USING DELTA;

-- Gold Layer: Vessel Disruption Risk
CREATE TABLE IF NOT EXISTS vessel_disruption_risk (
  MMSI BIGINT,
  VesselName STRING,
  VesselTypeName STRING,
  latest_lat DOUBLE,
  latest_lon DOUBLE,
  latest_sog DOUBLE,
  avg_sog DOUBLE,
  min_sog DOUBLE,
  max_sog DOUBLE,
  slowdown_events DOUBLE,
  port_zone STRING,
  distance_to_port_km DOUBLE,
  wind_speed_kmh DOUBLE,
  wind_gusts_kmh DOUBLE,
  temperature_c DOUBLE,
  precipitation_mm DOUBLE,
  visibility_m DOUBLE,
  wave_height_m DOUBLE,
  cloud_cover_pct DOUBLE,
  slowdown_score DOUBLE,
  weather_severity_score DOUBLE,
  weather_severity STRING,
  port_proximity_score DOUBLE,
  delay_risk_score DOUBLE,
  risk_level STRING COMMENT 'Critical | High | Medium | Low',
  recommended_action STRING,
  disruption_summary STRING
) USING DELTA;

-- Enhanced: Geopolitical Risk Zones
CREATE TABLE IF NOT EXISTS geopolitical_risk_zones (
  zone_name STRING,
  zone_type STRING,
  risk_level STRING,
  risk_score INT,
  description STRING,
  affected_routes STRING
) USING DELTA;

-- Enhanced: Maritime Obstacles
CREATE TABLE IF NOT EXISTS maritime_obstacles (
  obstacle_name STRING,
  obstacle_type STRING,
  port_zone STRING,
  radius_km INT,
  severity STRING,
  obstacle_risk_score INT,
  description STRING
) USING DELTA;

-- Enhanced: Safe Shipping Corridors
CREATE TABLE IF NOT EXISTS safe_corridors (
  corridor_name STRING,
  route_alternative STRING,
  avoids_zones STRING,
  additional_nm INT,
  additional_hours INT,
  safety_rating STRING,
  status STRING,
  description STRING
) USING DELTA;

-- Enhanced: Port Alternatives
CREATE TABLE IF NOT EXISTS port_alternatives (
  current_port STRING,
  alternative_port STRING,
  distance_nm INT,
  transit_hours DOUBLE,
  berth_capacity STRING,
  safety_rating STRING,
  status STRING,
  description STRING
) USING DELTA;

-- Enhanced: Vessel Routing Intelligence (computed from Gold + reference tables)
CREATE TABLE IF NOT EXISTS vessel_routing_intelligence (
  MMSI BIGINT,
  VesselName STRING,
  VesselTypeName STRING,
  latest_lat DOUBLE,
  latest_lon DOUBLE,
  latest_sog DOUBLE,
  avg_sog DOUBLE,
  slowdown_events DOUBLE,
  port_zone STRING,
  distance_to_port_km DOUBLE,
  wind_speed_kmh DOUBLE,
  wind_gusts_kmh DOUBLE,
  temperature_c DOUBLE,
  precipitation_mm DOUBLE,
  visibility_m DOUBLE,
  wave_height_m DOUBLE,
  cloud_cover_pct DOUBLE,
  slowdown_score DOUBLE,
  weather_severity_score DOUBLE,
  weather_severity STRING,
  port_proximity_score DOUBLE,
  delay_risk_score DOUBLE,
  risk_level STRING,
  recommended_action STRING,
  disruption_summary STRING,
  route_type STRING COMMENT 'Trans-Pacific | Trans-Atlantic | Gulf Route | Coastal',
  geopolitical_risk_score INT,
  obstacle_risk_score INT,
  fuel_cost_per_hour_usd INT,
  safety_score DOUBLE COMMENT '100 = safest',
  estimated_delay_hours DOUBLE,
  delay_cost_usd DOUBLE,
  rerouting_cost_usd DOUBLE,
  rerouting_recommended BOOLEAN,
  routing_priority STRING COMMENT 'Safety Critical | Balanced | Monitor'
) USING DELTA;
