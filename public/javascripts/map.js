angular.module("pinmap.map", ["leaflet-directive", "pinmap.common"])
  .controller("MapCtrl", function($scope, $timeout, leafletData, moduleManager) {

    $scope.times = [];
    $scope.timestamps = {};

    $scope.keyword = "";
    $scope.resultCount = 0;

    $scope.ws = new WebSocket("ws://" + location.host + "/ws");
    $scope.pinmapMapResul = [];

    $scope.sendQuery = function(e) {
      console.log("e = " + e);

      $scope.resultCount = 0;
      //var query = {offset: $scope.offset, limit: $scope.limit, byArray: true};
      var query = {byArray: true};
      if (e.keyword) {
        $scope.keyword = e.keyword;
        query["keyword"] = $scope.keyword;
      }
      if (e.slicingMode === "By-Interval") {
        query["mode"] = "interval"
      } else if (e.slicingMode === "By-Offset") {
        query["mode"] = "offset"
      }
      if (e.excludes) {
        query["excludes"] = true
      }
      $scope.timestamps.t0 = Date.now();
      console.log("sending query:");
      console.log(query);

      $scope.ws.send(JSON.stringify(query));
    };

    $scope.sendCmd = function(command) {
      var cmd = {cmd: command};
      $scope.ws.send(JSON.stringify(cmd));
    };

    $scope.waitForWS = function() {

      if ($scope.ws.readyState !== $scope.ws.OPEN) {
        window.setTimeout($scope.waitForWS, 1000);
      }
      else {
        //$scope.sendQuery();
        moduleManager.publishEvent(moduleManager.EVENT.WS_READY, {});
      }
    };

    // setting default map styles, zoom level, etc.
    angular.extend($scope, {
      tiles: {
        name: 'Mapbox',
        url: 'https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token={accessToken}',
        type: 'xyz',
        options: {
          accessToken: 'pk.eyJ1IjoiamVyZW15bGkiLCJhIjoiY2lrZ2U4MWI4MDA4bHVjajc1am1weTM2aSJ9.JHiBmawEKGsn3jiRK_d0Gw',
          id: 'jeremyli.p6f712pj'
        }
      },
      controls: {
        custom: []
      }
    });

    // initialize the leaflet map
    $scope.init = function() {
      leafletData.getMap().then(function (map) {
        $scope.map = map;
        $scope.bounds = map.getBounds();
        //making attribution control to false to remove the default leaflet sign in the bottom of map
        map.attributionControl.setPrefix(false);
        map.setView([$scope.lat, $scope.lng], $scope.zoom);
      });

      //Reset Zoom Button
      var button = document.createElement("a");
      var text = document.createTextNode("Reset");
      button.appendChild(text);
      button.title = "Reset";
      button.href = "#";
      button.style.position = 'inherit';
      button.style.top = '10%';
      button.style.left = '1%';
      button.style.fontSize = '14px';
      var body = document.body;
      body.appendChild(button);
      button.addEventListener("click", function () {
        $scope.map.setView([$scope.lat, $scope.lng], 4);
      });

      $scope.waitForWS();
      moduleManager.subscribeEvent(moduleManager.EVENT.CHANGE_SEARCH_KEYWORD, function(e) {
        $scope.cleanPinMap();
        $scope.sendQuery(e);
      });
    };

    $scope.handleResult = function(resultSet) {
      if(resultSet.length > 0) {
        $scope.pinmapMapResult = resultSet;
        $scope.resultCount += $scope.pinmapMapResult.length;
        moduleManager.publishEvent(moduleManager.EVENT.CHANGE_RESULT_COUNT, {resultCount: $scope.resultCount});
        $scope.timestamps.t9 = $scope.drawPinMap($scope.pinmapMapResult);
      }
    };

    $scope.ws.onmessage = function(event) {
      $timeout(function() {
        $scope.timestamps.t7 = Date.now();

        const response = JSONbig.parse(event.data);

        if (response.done) {
          console.log("-------- Done! --------");
          function toCSV(array) {
            var csv = "";
            array.forEach(function(row) {
              csv += row.join(",") + "\n";
            });
            return csv;
          }
          console.log(toCSV($scope.times));
          $scope.times = [];
          return;
        }

        $scope.timestamps.t8 = Date.now();

        //console.log("ws.onmessage <= " + JSON.stringify(response));

        $scope.timestamps.length = response.length;
        $scope.timestamps.t1 = response.t1;
        $scope.timestamps.T2 = response.T2;
        $scope.timestamps.T3 = response.T3;
        $scope.timestamps.T45 = response.T45;
        $scope.timestamps.T45i = response.T45i;
        $scope.timestamps.T6 = response.T6;
        $scope.timestamps.t6 = response.t6;

        $scope.handleResult(response.data);

        $scope.timestamps.keyword = $scope.keyword;
        $scope.times.push([
          $scope.keyword, // keyword
          $scope.timestamps.length,
          $scope.timestamps.t1 - $scope.timestamps.t0, // T1
          $scope.timestamps.T2, // T2
          $scope.timestamps.T3, // T3
          $scope.timestamps.T45, // T4+T5
          0,  // T4-parse
          0,  // T4-bind
          0,  // T4-execute
          0,  // T5
          $scope.timestamps.T45i, //T-insert
          $scope.timestamps.T6, // T6
          $scope.timestamps.t7 - $scope.timestamps.t6, // T7
          $scope.timestamps.t8 - $scope.timestamps.t7, // T8
          $scope.timestamps.t9 - $scope.timestamps.t8, // T9
          $scope.timestamps.t7 - $scope.timestamps.t0 // T1 + T2 + ... + T7
        ]);

        //console.log(JSON.stringify($scope.timestamps));
        //console.log(JSON.stringify($scope.times));
      });
    };

    // For randomize coordinates by bounding_box
    var randomizationSeed;

    // javascript does not provide API for setting seed for its random function, so we need to implement it ourselves.
    function CustomRandom() {
      var x = Math.sin(randomizationSeed++) * 10000;
      return x - Math.floor(x);
    }

    // return a random number with normal distribution
    function randomNorm(mean, stdev) {
      return mean + (((CustomRandom() + CustomRandom() + CustomRandom() + CustomRandom() + CustomRandom() + CustomRandom()) - 3) / 3) * stdev;
    }

    // randomize a pin coordinate for a tweet according to the bounding box (normally distributed within the bounding box) when the actual coordinate is not availalble.
    // by using the tweet id as the seed, the same tweet will always be randomized to the same coordinate.
    $scope.rangeRandom = function rangeRandom(seed, minV, maxV){
      randomizationSeed = seed;
      return randomNorm((minV + maxV) / 2, (maxV - minV) / 16);
    };

    // function for drawing pinmap
    $scope.drawPinMap = function(result) {

      //To initialize the points layer
      if (!$scope.pointsLayer) {
        $scope.pointsLayer = new WebGLPointLayer();
        $scope.pointsLayer.setPointSize(3);
        $scope.pointsLayer.setPointColor(0, 0, 255);
        $scope.map.addLayer($scope.pointsLayer);
        $scope.points = [];
      }

      //Update the points data
      if (result.length > 0) {
        // (1) returned by array - all coordinates of all records in one array
        if (result.byArray) {
          var coordinates = result.coordinates;
          var ids = result.ids;
          $scope.points = [];
          for (var i = 0; i < result.length; i++) {
            $scope.points.push([coordinates[i][1], coordinates[i][0], ids[i]]);
          }

          console.log("drawing points size = " + $scope.points.length);
          //console.log($scope.points);
          return $scope.pointsLayer.appendData($scope.points);
        }
        // (2) returned by json - each record in one json object
        else {
          $scope.points = [];
          for (let i = 0; i < result.length; i++) {
            if (result[i].hasOwnProperty("coordinate")) {
              $scope.points.push([result[i].coordinate[1], result[i].coordinate[0], result[i].id]);
            }
            else if (result[i].hasOwnProperty("place.bounding_box")) {
              $scope.points.push([$scope.rangeRandom(result[i].id, result[i]["place.bounding_box"][0][1], result[i]["place.bounding_box"][1][1]), $scope.rangeRandom(result[i].id + 79, result[i]["place.bounding_box"][0][0], result[i]["place.bounding_box"][1][0]), result[i].id]); // 79 is a magic number to avoid using the same seed for generating both the longitude and latitude.
            }
            else {
              $scope.points.push([result[i].y, result[i].x, result[i].id]);
            }
          }

          console.log("drawing points size = " + $scope.points.length);
          //console.log($scope.points);
          return $scope.pointsLayer.appendData($scope.points);
        }
      }

      return 0;
    };

    $scope.cleanPinMap = function() {
      $scope.points = [];
      if($scope.pointsLayer != null) {
        $scope.map.removeLayer($scope.pointsLayer);
        $scope.pointsLayer = null;
      }
    };
  })
  .directive("map", function () {
    return {
      restrict: 'E',
      scope: {
        lat: "=",
        lng: "=",
        zoom: "="
      },
      controller: 'MapCtrl',
      template:[
        '<leaflet lf-center="center" tiles="tiles" events="events" controls="controls" width="100%" height="100%" ng-init="init()"></leaflet>'
      ].join('')
    };
  });


angular.module("pinmap.map")
  .controller('CountCtrl', function($scope, moduleManager) {
    $scope.resultCount = 0;

    moduleManager.subscribeEvent(moduleManager.EVENT.CHANGE_RESULT_COUNT, function(e) {
      $scope.resultCount = e.resultCount;
    })
  });