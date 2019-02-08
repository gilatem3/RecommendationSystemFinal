angular.module('recEngineApp', [])
 .controller('recEngineController', function($location, $http, $q) {

  var engine = this;

  engine.pageTitle = 'Recommendation Engine!';

  // AWS API Gateway host + deployment
  var host = 'https://la16v542f3.execute-api.us-east-1.amazonaws.com/prod/';

  // API Gateway event API endpoint
  var endpoint = '/event/';

  // default event_id
  var defaultEventId = '57a805f4-eabe-11e7-a6bc-22000a5dcdc5';
  // a9126884-a92e-11e7-9b80-22000a0dd305
  //57a805f4-eabe-11e7-a6bc-22000a5dcdc5

  var base = host + endpoint;

  // a simple (if unattractive) way to grab an event id from /index.html?id=XXXXXXX
  var eventId = ($location.absUrl().split('='))[1];

  recommendations = [];
  engine.recommendations = [];

  $http.get(base + (eventId || defaultEventId))
   .then(
    function(response) {

     engine.event = response.data;

     console.error('found event ', response.data);

     // from here on out we assume that each event in DynamoDB contains
     // a "recommendations" element with a list of event ids
     angular.forEach(response.data.recommendations, function(rec) {
      recommendations.push($http.get(base + rec));
     });

     $q.all(recommendations)
      .then(function(responses) {
        angular.forEach(responses, function(response) {
         console.error('found recommendation ', response.data);
         engine.recommendations.push(response.data);
        })
       },
       function(error) {
        console.error('http GET error ', error);
       });
    },
    function(error) {
     console.error('http GET error ', error);
    });

 });