angular.module("pinmap.map", ["leaflet-directive", "pinmap.common"])
    .controller("MapCtrl", function($scope, $timeout, leafletData, moduleManager) {

        $scope.offset = 0;
        $scope.limit = 10000000; // 10M
        $scope.keyword = "";
        $scope.resultCount = 0;

        $scope.ws = new WebSocket("ws://" + location.host + "/ws");
        $scope.pinmapMapResul = [];

        $scope.sendQuery = function(keyword) {
            if (keyword && keyword !== $scope.keyword) {
                $scope.keyword = keyword;
                $scope.resultCount = 0;
                var query = {offset: $scope.offset, limit: $scope.limit, keyword: $scope.keyword};
                $scope.ws.send(JSON.stringify(query));
            }
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
                $scope.sendQuery(e.keyword);
            });
        };

        $scope.handleResult = function(resultSet) {
            if(angular.isArray(resultSet)) {
                $scope.pinmapMapResult = resultSet;
                if ($scope.pinmapMapResult.length > 0) {
                    $scope.resultCount += $scope.pinmapMapResult.length;
                    moduleManager.publishEvent(moduleManager.EVENT.CHANGE_RESULT_COUNT, {resultCount: $scope.resultCount});
                    $scope.drawPinMap($scope.pinmapMapResult);
                }
            }
        };

        $scope.ws.onmessage = function(event) {
            $timeout(function() {
                var result = JSONbig.parse(event.data);
                $scope.handleResult(result);
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
            var ret = randomNorm((minV + maxV) / 2, (maxV - minV) / 16);
            return ret;
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
                $scope.points = [];
                for (var i = 0; i < result.length; i++) {
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
                //$scope.pointsLayer.appendData($scope.points);
                console.log("drawing points size = " + $scope.points.length);
                console.log($scope.points);
                $scope.pointsLayer.setData($scope.points);
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