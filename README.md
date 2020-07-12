# gis-spark
Spark extension for normal spatio-temporal data analysis.

This project aims to make the most out of spark framework for professional GIS operations.

Unlike the previous framework (like GeoSpark, SpatialHadoop, etc), we hope that our framework's semantics are more in line with the world view of GIS.

This repository is still under developing.

## STData Reader and Writer：

- [x] Postgis
- [x] CSV (wkt)
- [x] Shp (only read)
- [x] MongoDB (only write)
- [x] ES

- [ ] layer transform to Dataset<Row>

## Gis Functions：  
### 1. Layer type preserved function

- [x] Transformation
- [x] Layer shift
- [x] Polygon makevalid 

### 2. Layer type changed funcion

- [x] Buffer

### 3. Layer interaction function

- [x] clip
- [x] intersect
- [x] erase

### 4. Layer visualization

- [ ] Pyramid Tile Set
- [ ] Heatmap

### 5. Moedls
- [x] Clip Model
- [x] Intersection Model
- [x] Erase Model
Partitial: Not indexed layer yet