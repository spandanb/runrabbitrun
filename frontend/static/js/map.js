/******************************************
              GLOBALS
******************************************/
var map
var markers = []
var insight = {lat: 37.4263597, lng: -122.1432371}
var user_id = null

//Synthetic data 
var sdata = {
    markers: [], 
    xstep: 0,
    ystep: 0,
    path: null, //synthesized path
    pidx: 0, //index of point in path
    job_id: null, //the id of the job that places markers
    running: false, //whether job is running
}

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

var uuid = function(){
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
        return v.toString(16);
    });
}

var tail = function(arr){
    //Returns tail element
    return arr[arr.length - 1]    
}


/*to the format expected by the backend, i.e.
    [lat, lon, 0, altitude, day since 12/30/1899, YYYY-MM-DD, HH:mm:SS]
*/
function tofmt(lat, lon){
    
    var datetime = new Date()
    var day = datetime.getFullYear() + "-" + datetime.getMonth() + "-" + datetime.getDate()
    var time = datetime.getHours() + ":" + datetime.getMinutes() + ":" + datetime.getSeconds()

    return [lat, lon, 0, 234, 42646, day, time, user_id]
}

//Generates a random walk
function randWalk(seed){

    var step = 0.0001 //step size
    var n = 1000 //number of steps

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

function nextStep(){
    var last = tail(sdata.path) 
    lat = last[0]
    lon = last[1]
    var x = sdata.xstep * randint(2, 10) + lat
    var y = sdata.ystep * randint(2, 10) + lon
    sdata.path.push(tofmt(x,y))
    return {lat: x, lng: y}
}


/******************************************
                 MAIN 
******************************************/
/*Do any initialization*/
(function(){
    user_id = uuid()
})()

/*
Place markers for synthetic data and update the text fields
*/
function synthData(){

    sdata.job_id = setInterval(function(){
        var position = nextStep() //next position

        var lat_vals = $("#lat-vals").val()
        $("#lat-vals").val(lat_vals + position.lat + ",")

        var lon_vals = $("#lon-vals").val();
        $("#lon-vals").val(lon_vals + position.lng + ",")
       
        addMarker(position);
        
    }, 400);
}


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
    for (var i = 0; i < path.length; i++) {
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

/******************************************
                 EVENTS
******************************************/
/*
Handles logic of creating synthetic data, populating input fields and dropping pins 
*/
$("#synth-data").click(function(){
    //Start a new job
    if(!sdata.running && !sdata.job_id){
        var seedlat = Number($("#seed-lat").val())
        var seedlon = Number($("#seed-lon").val()) 

        sdata.xstep = 0.00001 * (randbool()? 1: -1)
        sdata.ystep = 0.00001 * (randbool()? 1: -1)

        sdata.path = [tofmt(seedlat, seedlon)];
        sdata.running = true
        synthData()
    }
    //Stop the running job
    else if(sdata.running){
        clearInterval(sdata.job_id)
        sdata.running = false
    //Resume a job
    }else{
        sdata.running = true
        synthData()
    }
});

/*
* Recenters the map based on the seed values
*/
$("#center-map").click(function(){
    var lat = Number($("#seed-lat").val());
    var lon = Number($("#seed-lon").val());

    map.setCenter({lat: lat, lng: lon});
});
