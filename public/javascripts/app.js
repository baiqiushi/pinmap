if (window.console) {
  console.log("Welcome to your Play application's JavaScript!");
}

var app = angular.module("pinmap", ["pinmap.map", "pinmap.searchbar"]);

app.controller("AppCtrl", function ($scope) {
});