$(function(){

    var send_coord = function(lat, lon){
       $.ajax({
            url: "/coords",
            data: {"body": JSON.stringify({"lat": lat, "lon": lon})},
            method: "POST",
            success: function(resp){
                console.log(resp);
            }
        })
    }

    $("#submit-btn").click(function(e){
        var lat = $("#lat-input").val();
        var lon = $("#lon-input").val();
        //send_coord(lat, lon);
       drop() 
    });


})
