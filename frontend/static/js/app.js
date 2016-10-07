$(function(){

    var render_chart = function(data){
        /*
        data:- [series]
            series:- {name: <>, data: []}
                data:- [p0, p1, ...] or [[x0, y0], [x1, y1],...]
            //Note: the data series do not need to be same size, but they will be left justfied
        */
        
        var options = {
            chart: {
                renderTo: 'my-container',
                type: 'spline'
            },
            title: {
                text: 'Your Relative Speed (km/h)'
            },
            xAxis: {
                enabled: true,
                title: 'Points',
                allowDecimals: false
            },
            yAxis: {
                title: 'Speed',
                enabled: true,
            },
            plotOptions: {
                line: {
                    dataLabels: {
                        enabled: true
                    },
                    enableMouseTracking: false
                }
            },
            series: [],
        }

        options.series = data;

        var chart = new Highcharts.Chart(options);    
    }

    var data_fsm = {
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
                    //parse the JSON
                    //console.log(resp)
                    if (!$.isEmptyObject(resp)){
                        data_fsm.stop_loop()

                        var series = [{name: "Me", data: resp.path}]
                        for(var i=0; i<resp.matches.length; i++)
                            series.push({name: "User"+(i+1), data: resp.matches[i]})
                        
                        render_chart(series)

                        console.log(resp.matches) //[[point]]
                        console.log(resp.path)    //[point]
                    }
                }
            })
        },

        start_loop: function(){
            this.jobid = setInterval(this._get_response, 2000)
        },

        stop_loop: function(){
            clearInterval(this.jobid)
        },

    }

    $("#run-query").click(function(e){
        //Sends request
        var data = sdata.path
        data_fsm.send_coords(data)
        data_fsm.start_loop()
    });


})
