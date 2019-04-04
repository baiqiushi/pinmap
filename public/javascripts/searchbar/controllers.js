angular.module("pinmap.searchbar", ["pinmap.common"])
    .controller("SearchCtrl", function ($scope, moduleManager) {
        $scope.disableSearchButton = true;

        $scope.search = function () {
            if ($scope.keyword && $scope.keyword.trim().length > 0) {
                //Splits out all individual words in the query keyword.
                var keywords = $scope.keyword.trim().split(/\s+/);

                if (keywords.length === 0) {
                    alert("Your query only contains stopwords. Please re-enter your query.");
                } else {
                    moduleManager.publishEvent(moduleManager.EVENT.CHANGE_SEARCH_KEYWORD,
                        {keyword: keywords[0], slicingMode: $scope.slicingMode, excludes: $scope.excludes});
                }
            }
            // No keyword query
            else {
                moduleManager.publishEvent(moduleManager.EVENT.CHANGE_SEARCH_KEYWORD,
                  {slicingMode: $scope.slicingMode, excludes: $scope.excludes});
            }
        };

        moduleManager.subscribeEvent(moduleManager.EVENT.WS_READY, function(e) {
            $scope.disableSearchButton = false;
        });

        $scope.slicingModes = ["No-Slicing", "By-Interval", "By-Offset"];
    })
    .directive("searchBar", function () {
        return {
            restrict: "E",
            controller: "SearchCtrl",
            template: [
                '<form class="form-inline" id="input-form" ng-submit="search()" >',
                '<div class="input-group col-lg-12">',
                '<select ng-model="slicingMode" ng-options="x for x in slicingModes" ng-init="slicingMode = slicingModes[1]"></select>',
                '<label>Excludes On: <input type="checkbox" ng-model="excludes"></label>',
                '<label class="sr-only">Keywords</label>',
                '<input type="text" style="width: 97%" class="form-control " id="keyword-textbox" placeholder="Search keywords, e.g. hurricane" ng-model="keyword"/>',
                '<span class="input-group-btn">',
                '<button type="submit" class="btn btn-primary" id="submit-button" ng-disabled="disableSearchButton">Submit</button>',
                '</span>',
                '</div>',
                '</form>'
            ].join('')
        };
    });
