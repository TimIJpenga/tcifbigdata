var app = angular.module('bigData', []);

app.controller('controller', function($scope, $http) {

    $scope.abc = ['a', 'b', 'c', 'd', 'e'];

    $http.get("../result/letter_patronen/result.json").then(function(data) {
        $scope.data = data.data;
    });

});