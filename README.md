# pinmap

## Overview
 - Using PlayFramework 2.6
 - Using Leaflet for showing map
 - Using `webgl_points_layer.js` library for showing points

## Run
```
sbt "project pinmap" "run 9001"
```

## Feature
 - When http://localhost:9001 accessed
 - Frontend `map.js` ssend request to ws://localhost:9000/ws for 500 points with offset 0
 - Backend `HomeController` loads `pointsData.txt` (5k points json data) into memory and handles request
 - Once `map.js` receives the result, sends the next query with 500 length and offset respectively
