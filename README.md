# Supply Chain Disruption Intelligence Platform

Real-time maritime supply chain disruption intelligence system built on Databricks. Tracks vessel delays, weather risks, geopolitical threats, and provides AI-powered routing recommendations with cost/safety optimization.

[![Databricks](https://img.shields.io/badge/Databricks-Data%20Platform-red)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Lakehouse-blue)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3.12-green)](https://python.org/)

## 🚢 Overview

This platform provides real-time intelligence for supply chain disruption management, leveraging:
- **Synthetic AIS vessel tracking** (200 vessels, 57,600+ positions, 3-day window)
- **Live weather enrichment** via Open-Meteo API
- **Geopolitical risk analysis** (Red Sea attacks, South China Sea disputes, sanctions zones)
- **Maritime obstacle tracking** (storms, ice, military zones, oil spills)
- **AI-powered routing recommendations** with safety/cost optimization
- **Interactive AI/BI Dashboard** (28 widgets, real-time KPIs, scatter plots, heatmaps)

## 📊 Architecture

### Medallion + Enhanced Intelligence

```
┌─────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER                              │
│  workspace.supply_chain.ais_bronze                              │
│  - Synthetic AIS positions (MMSI, LAT, LON, SOG, timestamp)    │
│  - 200 vessels × 72 hours = 57,600 records                     │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER                              │
│  workspace.supply_chain.ais_silver                              │
│  - Deduplication on MMSI + timestamp                            │
│  - Timestamp parsing, vessel type mapping                       │
│  - Geo filtering (LAT 20-60, LON -130 to -65)                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER                               │
│  workspace.supply_chain.vessel_disruption_risk                  │
│  - Per-vessel aggregation with risk scoring                     │
│  - Weather enrichment (Open-Meteo API)                          │
│  - Composite risk: slowdown (35%) + weather (40%) + port (25%) │
│  - AI-generated disruption summaries                            │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                    ENHANCED INTELLIGENCE                         │
│  workspace.supply_chain.vessel_routing_intelligence             │
│  - Geopolitical risk scores (Red Sea, South China Sea, etc.)   │
│  - Maritime obstacle impacts (storms, ice, military zones)      │
│  - Safety scores (crew/vessel first priority)                   │
│  - Cost modeling (delay cost, rerouting cost, net savings)     │
│  - Live routing recommendations with priority classification    │
└─────────────────────────────────────────────────────────────────┘
```

### Reference Tables
- **port_zones** (6 US port zones with bounding boxes)
- **geopolitical_risk_zones** (7 war/conflict/sanctions zones)
- **maritime_obstacles** (8 synthetic obstacles)
- **safe_corridors** (8 alternative shipping routes)
- **port_alternatives** (12 nearby diversion ports)

## 🎯 Key Features

### Risk Scoring Formula
```python
delay_risk_score = (
    slowdown_score * 0.35 +      # Vessel speed reduction
    weather_severity_score * 0.40 # Wind, waves, visibility
    + port_proximity_score * 0.25 # Congestion proximity
)
```

### Safety Score Formula
```python
safety_score = 100 - (
    delay_risk_score * 0.30 +
    geopolitical_risk_score * 0.35 +
    obstacle_risk_score * 0.20 +
    weather_severity_score * 0.15
)
```

### Routing Priority
1. **Safety Critical** — Immediate rerouting required (geopolitical/weather)
2. **Balanced** — Cost-benefit favors diversion
3. **Cost Optimized** — Monitor for minor route adjustments
4. **Monitor** — No action required

## 📈 Dashboard

**28 Widgets** organized across operational intelligence, routing recommendations, and geopolitical analysis:

### KPIs (7)
- Total Vessels Monitored
- Critical Risk Count
- High Risk Count
- Average Risk Score
- Total Delay Cost
- Net Savings from Rerouting
- Vessels Needing Rerouting

### Visualizations (21)
- High Risk Vessels (table with MMSI, port, risk score, weather)
- Risk Level Distribution (pie chart)
- Port Zone Risk Heatmap (bar chart)
- Weather Severity Distribution (bar chart)
- Weather Impact on Risk (scatter plot: wind vs delay risk)
- Vessel Speed vs Risk (scatter plot: SOG vs delay risk)
- Geopolitical Risk by Port Zone
- Obstacle Risk by Port Zone
- Cost vs Safety Trade-off (scatter plot)
- Live Routing Recommendations (table with priority, cost, safety)
- Route Options Comparison (table)
- And more...

**Dashboard URL**: [View Published Dashboard](https://dbc-cf1b98fc-35d7.cloud.databricks.com/dashboardsv3/01f1a6b7e73c120f9949d594c1b90515/published)

## 🛠️ Setup

### Prerequisites
- Databricks workspace (Unity Catalog enabled)
- SQL Warehouse or Serverless compute
- Internet access (Open-Meteo API)

### Installation

1. **Clone this repository**
   ```bash
   git clone https://github.com/vikash-aryan/supply-chain-disruption-intelligence.git
   ```

2. **Import notebook to Databricks**
   - Upload `pipeline.py` to your workspace
   - Attach to a cluster with Python 3.12+

3. **Run the pipeline**
   ```python
   # Execute all cells in order
   # Creates 9 tables in workspace.supply_chain schema
   ```

4. **Import dashboard** (optional)
   - Dashboard JSON available in `docs/` folder
   - Or recreate from `sql/` scripts using AI/BI Dashboard editor

### Data Generation
All data is **fully synthetic**:
- AIS positions generated with realistic port zones, speeds, and timestamps
- Weather data from **Open-Meteo API** (free, no license required)
- Geopolitical zones, obstacles, and routing intelligence are creative scenarios
- No licensed datasets (NOAA, MarineTraffic, Lloyd's List, etc.) used

## 📊 Tables Created

| Table | Rows | Description |
|-------|------|-------------|
| `ais_bronze` | ~57,600 | Synthetic AIS vessel positions |
| `ais_silver` | ~57,600 | Cleaned and deduplicated AIS |
| `vessel_disruption_risk` | 200 | Gold layer with risk scores |
| `port_zones` | 6 | US port zones reference |
| `geopolitical_risk_zones` | 7 | War/conflict/sanctions zones |
| `maritime_obstacles` | 8 | Storms, ice, military zones |
| `safe_corridors` | 8 | Alternative shipping routes |
| `port_alternatives` | 12 | Nearby diversion ports |
| `vessel_routing_intelligence` | 200 | Enhanced with routing recommendations |

## 🔧 Usage

### Query High-Risk Vessels
```sql
SELECT 
  VesselName, port_zone, risk_level, 
  delay_risk_score, safety_score,
  rerouting_recommended
FROM workspace.supply_chain.vessel_routing_intelligence
WHERE risk_level IN ('Critical', 'High')
ORDER BY delay_risk_score DESC;
```

### Find Vessels Needing Rerouting
```sql
SELECT 
  VesselName, route_type, routing_priority,
  delay_cost_usd, rerouting_cost_usd, net_savings_usd,
  rerouting_recommendation
FROM workspace.supply_chain.vessel_routing_intelligence
WHERE rerouting_recommended = true
ORDER BY net_savings_usd DESC;
```

### Geopolitical Risk Analysis
```sql
SELECT 
  zone_name, risk_level, risk_score,
  description, affected_routes
FROM workspace.supply_chain.geopolitical_risk_zones
WHERE risk_level = 'Critical'
ORDER BY risk_score DESC;
```

## 🧪 Testing

Run verification cell to confirm all tables exist:
```python
tables = [
    'workspace.supply_chain.ais_bronze',
    'workspace.supply_chain.ais_silver',
    'workspace.supply_chain.vessel_disruption_risk',
    'workspace.supply_chain.port_zones',
    'workspace.supply_chain.geopolitical_risk_zones',
    'workspace.supply_chain.maritime_obstacles',
    'workspace.supply_chain.safe_corridors',
    'workspace.supply_chain.port_alternatives',
    'workspace.supply_chain.vessel_routing_intelligence',
]

for t in tables:
    count = spark.table(t).count()
    print(f"{t}: {count} rows")
```

## 📚 Documentation

- [Pipeline Architecture](docs/architecture.md)
- [Risk Scoring Methodology](docs/risk_scoring.md)
- [API Integration Guide](docs/api_integration.md)
- [Dashboard Guide](docs/dashboard.md)

## 🤝 Contributing

This is a POC/demo project. For production use:
1. Replace synthetic AIS data with real feed (NOAA, MarineTraffic API)
2. Add streaming ingestion (Auto Loader + Structured Streaming)
3. Implement incremental processing (Delta Live Tables)
4. Add data quality monitoring (Databricks DQM)
5. Expand geopolitical intelligence (live news feeds, IMO alerts)

## 📝 License

MIT License - see LICENSE file for details

## 🙏 Acknowledgments

- **Open-Meteo** for free weather API
- **NOAA Marine Cadastre** for AIS schema reference (publicly documented)
- **Databricks** for the unified data platform

## 📧 Contact

**Author**: Vikash Aryan  
**Email**: vikasharyan333@gmail.com  
**GitHub**: [@vikash-aryan](https://github.com/vikash-aryan)

---

**Built with ❤️ on Databricks**
