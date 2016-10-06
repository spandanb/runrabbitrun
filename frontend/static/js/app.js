$(function(){

    var dataFSM = {
        //Send the request
        send_coords: function(data){
            $.ajax({
                url: "/coords",
                data: {"body": JSON.stringify(data), "user_id": user_id},
                method: "POST",
                success: function(resp){
                    console.log(resp);
                }
            })
        },

        _get_response: function(){
            console.log("Running _get_response")
            $.ajax({
                url: "/resp",
                data: {"user_id": user_id},
                method: "POST",
                success: function(resp){
                    console.log(resp);
                }
            })
        },

        start_loop: function(){
            this.jobid = setInterval(this._get_response, 2000)
        },

        stop_loop: function(){
            clearInterval(this.jobid)
        }

    }

    $("#run-query").click(function(e){
        //Sends request
        var data = sdata.path
        dataFSM.send_coords(data)
        dataFSM.start_loop()
    });


})
