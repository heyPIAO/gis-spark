# gis-spark
Spark extension for normal spatio-temporal data

This project aims to make the most out of spark framework for professional GIS operation.

Unlike the previous framework (like GeoSpark, SpatialHadoop, etc), we hope that the semantics of the framework is more in line with the world view of GIS.

This repository is still under developing.

## STData Reader and Writer：

- [x] Postgis
- [x] CSV (wkt)
- [x] Shp (only read)
- [x] MongoDB (only write)
- [x] ES

- [ ] layer 转成 Typed Dataset

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
- [ ] erase

### 4. Layer visualization

- [ ] Pyramid Tile Set