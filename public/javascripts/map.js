angular.module('pinmap.map', ['leaflet-directive'])
    .controller('MapCtrl', function($scope, $timeout, leafletData) {

        $scope.totalCount = 5000;
        $scope.deltaCount = 500;
        $scope.offset = 0;

        $scope.ws = new WebSocket("ws://" + location.host + "/ws");
        $scope.pinmapMapResul = [];

        $scope.sendQuery = function() {
            var query = {offset: $scope.offset, size: $scope.deltaCount};
            $scope.ws.send(JSON.stringify(query));
            $scope.offset = $scope.offset + $scope.deltaCount;
        };

        $scope.waitForWS = function() {

            if ($scope.ws.readyState !== $scope.ws.OPEN) {
                window.setTimeout($scope.waitForWS, 1000);
            }
            else {
                $scope.sendQuery();
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
        };

        $scope.handleResult = function(resultSet) {

            if(angular.isArray(resultSet)) {
                $scope.pinmapMapResult = resultSet;
                //send next query
                if ($scope.pinmapMapResult.length > 0) {
                    $scope.drawPinMap($scope.pinmapMapResult);
                    if ($scope.offset < $scope.totalCount) {
                        $scope.sendQuery();
                    }
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
                $scope.pointsLayer.setPointColor(29, 161, 242);
                $scope.map.addLayer($scope.pointsLayer);
                $scope.points = [];
            }

            //Update the points data
            if (result.length > 0){
                $scope.points = [];
                for (var i = 0; i < result.length; i++) {
                    if (result[i].hasOwnProperty("coordinate")){
                        $scope.points.push([result[i].coordinate[1], result[i].coordinate[0], result[i].id]);
                    }
                    else if (result[i].hasOwnProperty("place.bounding_box")){
                        $scope.points.push([$scope.rangeRandom(result[i].id, result[i]["place.bounding_box"][0][1], result[i]["place.bounding_box"][1][1]), $scope.rangeRandom(result[i].id + 79, result[i]["place.bounding_box"][0][0], result[i]["place.bounding_box"][1][0]), result[i].id]); // 79 is a magic number to avoid using the same seed for generating both the longitude and latitude.
                    }
                }
                $scope.pointsLayer.appendData($scope.points);
                //$scope.pointsLayer.setData($scope.points);
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


angular.module('pinmap.map')
    .controller('CountCtrl', function($scope) {
        $scope.resultCount = 0;
    });