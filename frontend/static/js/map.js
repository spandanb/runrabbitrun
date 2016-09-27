/******************************************
              GLOBALS
******************************************/
var map;
var markers = []
var insight = {lat: 37.4263597, lng: -122.1432371}

/******************************************
             UTILITY FUNCS 
******************************************/
var randbool = function(){
    //Returns a uniformly distributed bool
    return Math.random() >= 0.5
}

var randint = function(start, end){
    //returns a unif int between [start, end]
    return Math.round(Math.random() * (end - start) + start)   
}

/******************************************
                 MAIN 
******************************************/
//Generates a random walk
function randWalk(seed){

    var step = 0.0001 //step size
    var n = 5 //number of steps

    var xstep = step * (randbool()? 1: -1)
    var ystep = step * (randbool()? 1: -1)

    //seed it with the insight location 
    var posvect = [seed]
    for(var i=1; i< n; i++){
        var x = xstep * randint(-1, 5) + posvect[i-1]['lat']
        var y = ystep * randint(-1, 5) + posvect[i-1]['lng']
        posvect.push({lat: x, lng: y})
    }
    return posvect;
}

/*
Handles logic of creating synthic data, populating input fields and dropping pins 
*/
$("#synth-data").click(function(e){
    var seed = {
                    lat: Number($("#seed-lat").val()), 
                    lng: Number($("#seed-lon").val()) 
                }

    //The synthetic path
    var synth_path = randWalk(seed);
    var spidx = 0; //The synthetic path index

    var intervalID = setInterval(function(){
        var lat_vals = $("#lat-vals").val();
         $("#lat-vals").val(lat_vals + "," + synth_path[spidx].lat)

        var lon_vals = $("#lon-vals").val();
        $("#lon-vals").val(lon_vals + "," + synth_path[spidx].lat)
       
        addMarker(synth_path[spidx]);
 
        spidx += 1;
        if (spidx == synth_path.length) clearInterval(intervalID);
        
    }, 2000);
});

//Initialize Google Map
function initMap() {
    var lat = Number($("#seed-lat").val());
    var lon = Number($("#seed-lon").val());
    map = new google.maps.Map(document.getElementById('map'), {
        center: {lat: lat, lng: lon},
        zoom: 18
    });
}

//Animates the timed drop of many points
function drop() {
    var path = randWalk(insight);
    clearMarkers();
    for (var i = 0; i < path2.length; i++) {
        addMarkerWithTimeout(path[i], i * 800);
    }
}

//Adds marker instanteously
function addMarker(position) {
    markers.push(new google.maps.Marker({
        position: position,
        map: map,
        animation: google.maps.Animation.BOUNCE
    }));
}

//Adds marker after some delay 
function addMarkerWithTimeout(position, timeout) {
        window.setTimeout(function() {
          markers.push(new google.maps.Marker({
            position: position,
            map: map,
            //animation: google.maps.Animation.BOUNCE
          }));
        }, timeout);
}

function clearMarkers() {
        for (var i = 0; i < markers.length; i++) {
          markers[i].setMap(null);
        }
        markers = [];
}
