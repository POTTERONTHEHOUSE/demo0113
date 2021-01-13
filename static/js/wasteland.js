 phase="not started";
 workload=0;
 metric=0;
 x = Math.floor(Math.random() * 1000000000);  
 timestamp=0; 
 init_timestamp=0;
 rocks=0;
 i=0;
 j=0;
//datset

var param_json= {
        "giraph": {
            "dblp": {
                "ppr": {
                    "1": {
                        "HWM": [
                            12123.564242008973,
                            0.9947808965676657,
                            4489029.272243716
                        ],
                        "RSS": [
                            13774.149105002669,
                            0.9947808965676656,
                            354101.80242175155
                        ]
                    },
                    "2": {
                        "HWM": [
                            537.3936843232316,
                            1.499999999988984,
                            4680904.40810257
                        ],
                        "RSS": [
                            6958.749552429689,
                            0.9925708866644026,
                            183401.92749056735
                        ]
                    },
                    "4": {
                        "HWM": [
                            7100.133007608205,
                            0.9689842181700475,
                            3293829.9008468767
                        ],
                        "RSS": [
                            4099.7559003583965,
                            0.9689842181700474,
                            92948.70177987039
                        ]
                    },
                    "8": {
                        "HWM": [
                            3277.746395777912,
                            0.9882617777512578,
                            3059528.4863392706
                        ],
                        "RSS": [
                            1852.8664977384315,
                            0.9882617777512577,
                            51576.63366525556
                        ]
                    }
                },
                "sssp": {}
            },
            "web_stanford": {
                "ppr": {
                    "1": {
                        "HWM": [
                            14820.374086882055,
                            1.0232260194356881,
                            2687188.139685782
                        ],
                        "RSS": [
                            4977.894568048539,
                            1.023226019435688,
                            180475.6045679739
                        ]
                    },
                    "2": {
                        "HWM": [
                            7241.9765831381255,
                            0.9870948103192878,
                            3188383.2694713627
                        ],
                        "RSS": [
                            3012.577681874892,
                            0.9870948103192877,
                            94301.8915799661
                        ]
                    },
                    "4": {
                        "HWM": [
                            15822.324528034715,
                            0.8357894729798752,
                            2645886.1798614464
                        ],
                        "RSS": [
                            4181.0869774054345,
                            0.835789472979875,
                            30941.833874304615
                        ]
                    },
                    "8": {
                        "HWM": [
                            19203.503489437287,
                            0.7623544618246741,
                            1923093.9439871497
                        ],
                        "RSS": [
                            4407.848754133674,
                            0.7623544618246739,
                            8.517118650553968e-11
                        ]
                    }
                },
                "sssp": {}
            }
        },
        "pregelplus": {
            "dblp": {
                "sssp":{
                    "8":{
                        "HWM": [  2.45655069e+04  , 9.03500372e-01  , 2.39708567e+04],
                        "RSS": [  2.70268924e+04 ,  8.85778265e-01,  -4.77890146e+03]
                    },
                    "4":{
                        "HWM": [48731.1505837,0.913100055195,114603.698565],
                        "RSS": [55184.62948,0.882603107527,71559.6792859]
                    },
                    "2":{
                        "HWM": [71912.6718299,0.992130405323,293373.691548],
                        "RSS": [69111.5334377,0.986168764434,216709.588885]
                    },
                    "1":{
                        "HWM": [  1.50122859e+05,   9.30127652e-01,   5.93915367e+05],
                        "RSS": [  1.72557543e+05,   9.03204288e-01,   3.39637652e+05]
                    },

                    "Storage": [ 6225548.7182578,  4281906.27975899]
                },
                "ppr":{
                    "8":{
                        "HWM": [2976.29676713,0.941363987519,88703.6523338],
                        "RSS": [4998.04713725,0.788039378763,65784.2005803]
                    },
                    "4":{

                        "HWM":  [8700.16634599,0.896189217657,77311.9210223],
                        "RSS":  [12048.9374041,0.76103338475,83303.5834426]
                    },
                    "2":{

                        "HWM": [11277.9674978,0.996521395654,203915.239871] ,
                        "RSS": [17604.2696648,0.807924442717,203221.993647]

                    },
                    "1":{
                        "HWM": [20967.0294259,0.960520825379,309742.701645],
                        "RSS": [56700.9627489,0.733385265235,202043.470701]
                    },

                    "Storage":[  1.75741133e+06 , 2.20444411e+08]

                }
            },
            "web_stanford":{
                "ppr":{
                    "8":{
                        "HWM": [1.23458777e+03, 9.58774267e-01, 5.12054549e+04],
                        "RSS": [1.50809134e+03, 8.41440759e-01, 7.26332939e+04]
                    },
                    "4":{
                        "HWM": [2897.55903429,0.946701419138,63859.0262229],
                        "RSS": [5623.61964329,0.738588238457,66526.1228284]
                    },
                    "2":{
                        "HWM": [5464.98144691,0.976352686117,109861.511458],
                        "RSS":  [20178.268208,0.630509961475,77113.2988232]
                    },

                    "1":{
                        "HWM": [7456.0805296,1.00650870104,232176.766905],
                        "RSS":  [40235.8684908,0.621617055653,142273.587871]
                    },
                    "Storage":[  4.63707213e+05 ,  1.24202858e+08]
                },
                "sssp":{
                    "8":{
                        "HWM": [1343.21634052,1.19184491319,84519.7337351],
                        "RSS": [1429.57553497,1.18079489673,79933.3835136]
                    },
                    "4":{

                        "HWM": [4611.01703901,1.12519684607,95980.3623831] ,
                        "RSS": [5258.57928549,1.09779296326,86556.3210101]
                    },
                    "2":{

                        "HWM": [  2.27411103e+03,   1.38880527e+00,   3.89249592e+05] ,
                        "RSS": [  2.17847303e+03,   1.38534020e+00,   3.68463202e+05]

                    },
                    "1":{
                        "HWM": [  1.93147068e+04,   9.99733027e-01,   3.79456284e+05],
                        "RSS": [25411.6379, 0.953358365, 215328.125]
                    },

                    "Storage": [ 2837786.81484358,  7205432.70747437]

                }
            }
        }
    }
;

var batch_array=[];

var color_table=["#80ff8b","#80fff9","#8091ff","#b980ff","#ff8080","#ffb980","#edfa00","#00fa32","#0021fa","#8900fa","#c7041e"];

//system
/* if hidetable is called then togglesys will be needed
function togglesys(){
  $("#syscontent").collapse("toggle");
}*/
/*
function hidetables() {
  if(document.getElementById("graphd11").checked == true){
    //hide footprint
              $("#result-table12").hide();
              $("#result-table22").hide();
              $("#result-table11").show();
              $("#result-table21").show();
              document.getElementById("sysresult").innerHTML="GRAPHD";
            }
          else if (document.getElementById("graphd12").checked == true) {
    //hide i/o
            $("#result-table11").hide();
            $("#result-table21").hide();
            $("#result-table12").show();
            $("#result-table22").show();
            document.getElementById("sysresult").innerHTML="PREGELPLUS";
          }
      
}
*/
// for system select&form-control 
function switchtable(value) {  
  var selectedOption=value.options[value.selectedIndex];  
	console.log(selectedOption.value);
	if(selectedOption.value=="graphd"){
    $("#result-table12").hide();
    $("#result-table22").hide();
    $("#result-table11").show();
    $("#result-table21").show();
    
 	}else if(selectedOption.value=="pregelplus"){
    $("#result-table11").hide();
    $("#result-table21").hide();
    $("#result-table12").show();
    $("#result-table22").show();
    
   }   
}


//algorithm
function togglealg(){
  $("#algcontent").collapse("toggle");
}

function showalg1(){
  if(document.getElementById("algo11").checked == true){
    document.getElementById("algresult").innerHTML="MSSP";
  }
}
function showalg2(){
  if (document.getElementById("algo12").checked == true){
    document.getElementById("algresult").innerHTML="PPR";
  }
}

//cluster size
function toggleclusize(){
  var div1 = document.getElementById("clusizecontent");
  if (div1.style.display === "none") {
      div1.style.display = "block";
  } else {
      div1.style.display = "none";
  }
}

function showclusize1(){
  if(document.getElementById("machine11").checked == true){
    document.getElementById("clusizeresult").innerHTML="1";
  }
}
function showclusize2(){
  if (document.getElementById("machine12").checked == true){
    document.getElementById("clusizeresult").innerHTML="2";
  }
}
function showclusize3(){
  if(document.getElementById("machine13").checked == true){
    document.getElementById("clusizeresult").innerHTML="3";
  }
}
function showclusize4(){
  if (document.getElementById("machine14").checked == true){
    document.getElementById("clusizeresult").innerHTML="4";
  }
}
function showclusize5(){
  if(document.getElementById("machine15").checked == true){
    document.getElementById("clusizeresult").innerHTML="5";
  }
}
function showclusize6(){
  if (document.getElementById("machine16").checked == true){
    document.getElementById("clusizeresult").innerHTML="6";
  }
}
function showclusize7(){
  if(document.getElementById("machine17").checked == true){
    document.getElementById("clusizeresult").innerHTML="7";
  }
}
function showclusize8(){
  if (document.getElementById("machine18").checked == true){
    document.getElementById("clusizeresult").innerHTML="8";
  }
}
function showclusize9(){
  if (document.getElementById("machine19").checked == true){
    document.getElementById("clusizeresult").innerHTML=document.getElementById("usermachinenumber").value;
  }
}

 function function1(x){
     if (phase=="end"){return;}
     $.ajax({
         url:'http://localhost:80/status_query',
         async:false,//for the return value
         type:'POST',
         contentType: "application/json",
         data:JSON.stringify({
             "uuid":x,
         }),
         success : function(data){
             var count = data.result;

             phase=count[0].phase;
             workload=count[0].workload;
             metric=count[0].metric;
             var newtimestamp=Date.now()/1000;

             if (document.getElementById("chsys").value=="pregelplus"&&phase=="training"){
                 //count the number of the training batch
                 j++;
                 var table_body = $("#table-body_pregelplus_tra");
                 $("#pending-row").remove();
                 var tr = $('<tr>' + '<td>' + '<img src="/static/images/check4.png" style="width:20px;margin-right:10px; float:left;"/>' + ' Batch ' + j + '</td><td>'  + workload + '</td><td>' + metric + '</td><td>'  + metric + '</td><td>' + (newtimestamp-timestamp)  + '</td><td>'+ (newtimestamp-init_timestamp) + '</td></tr>');
             }
             if (document.getElementById("chsys").value=="pregelplus"&&phase!="training"){
                 i++;
                 table_body= $("#table-body_pregelplus_eval");
                 $("#pending-row").remove();
                 var tr = $('<tr>' + '<td>' + '<img src="/static/images/check4.png" style="width:20px;margin-right:10px; float:left;"/>' + ' Batch ' + i + '</td><td>'  + workload + '</td><td>' + metric + '</td><td>'  + metric + '</td><td>' + (newtimestamp-timestamp) + '</td><td>'+ (newtimestamp-init_timestamp) + '</td></tr>');
             }
             else if (document.getElementById("chsys").value=="graphd"&&phase=="training"){
                 //count the number of the training batch
                 j++;
                 table_body= $("#table-body_graphd_tra");
                 $("#pending-row").remove();
                 var tr = $('<tr>' + '<td>' + '<img src="/static/images/check4.png" style="width:20px;margin-right:10px; float:left;"/>' + ' Batch ' + j + '</td><td>'  + workload + '</td><td>' + metric + '</td><td>'+ (newtimestamp-timestamp) + '</td><td>'+ (newtimestamp-init_timestamp) + '</td></tr>');
             }
             else if (document.getElementById("chsys").value=="graphd"&&phase!="training"){
                 i++;
                 table_body= $("#table-body_graphd_eval");
                 $("#pending-row").remove();
                 var tr = $('<tr>' + '<td>' + '<img src="/static/images/check4.png" style="width:20px;margin-right:10px; float:left;"/>' + ' Batch ' + i + '</td><td>'  + workload + '</td><td>' + metric + '</td><td>'+ (newtimestamp-timestamp) + '</td><td>'+ (newtimestamp-init_timestamp) + '</td></tr>');
             }

             timestamp=newtimestamp;
             table_body.append(tr);
             //and then append the loading icon in the following row
             var trx = $('<tr id="pending-row">' + '<td>' + '<img src="/static/images/load2.gif"/>' +  '</td><td>'  + '</td><td>' +  '</td><td>' +  '</td><td>' +  '</td><td>' + '</td></tr>');
             table_body.append(trx);
         },

     })
 }

//workload
function togglewl(){
  $("#wlcontent").collapse("toggle");
}

//when the "disable atom" is chosen, the block switches to "manual input"; 
// for workload select&form-control 
function switchatom() {  
  var div1 = document.getElementById("toatom");
  var div2 = document.getElementById("tomanual");
  var div3 = document.getElementById("enatom");
  if (div3.value == "Enable") {
      div1.style.display = "block";
      div2.style.display = "none";
  } else {
      div1.style.display = "none";
      div2.style.display = "block";
  } 
}
//then by clicking the spinning icon (with a tooltip) users can switch back to the original.
function switchmanual() {  
  var div1 = document.getElementById("toatom");
  var div2 = document.getElementById("tomanual");
  if (div2.style.display === "none") {
      div2.style.display = "block";
      div1.style.display = "none";
  } else {
      div2.style.display = "none";
      div1.style.display = "block";
  } 
}

function checkworkload(){
  var x = new Number;
  x = Number(document.getElementById("workload").value);
  if (isNaN(x) || x < 1 || x > 40960) {
    window.alert("Illegal workload! Please enter again!");
   } 
   else {
      window.alert("Success");
   }
  document.getElementById("wlresult").innerHTML=document.getElementById("workload").value;
}



// batchnumber input value alert
function checkbatchnumber() {
  var x = new Number;
  x = Number(document.getElementById("userbatchnumber").value);
  if (isNaN(x) || x < 1 || x > document.getElementById("workload").value) {window.alert("Illegal workload! Please enter again!");} else {
      //window.alert("Success");
      generate_batch_inputs(x);
  }
}

// delete the last row for the tables in the rockit function
function deleterow(x) {
    var table = document.getElementById(x);
    console.log("table1",table);
    var rowCount = table.rows.length;

  table.deleteRow(rowCount -1);
  console.log("tabledeleterow",table);
  console.log(rowCount);
  
}

//server interface
function load_batch(workload, batch_num) {
    let array = new Array();
    for (var i = 0; i < batch_num; i++)
        array.push(parseInt(document.getElementById("batch_input_" + i).value));
    batch_array.push(array);
}

//ROCK IT
function draw_batch() {
    document.getElementById('batch_table').innerHTML = "";
    for(var i=0; i<batch_array.length; i++){
        var max_workload=2000;
        var batch_line=batch_array[i];
        console.log(batch_line);
        var batch_row=document.createElement("div");
        batch_row.setAttribute("class","myth_line");
        batch_row.setAttribute("title",i);
        for(var j=0; j<batch_line.length; j++){
            var batch_block=document.createElement("div");
            batch_block.setAttribute("class","myth");
            batch_block.setAttribute("style","width:"+790*batch_line[j]/max_workload+"px; background-color:"+color_table[i%color_table.length]);
            batch_block.innerText=batch_line[j];
            batch_row.append(batch_block);
        }
        document.getElementById('batch_table').append(batch_row);
    }
    plot_batch(batch_array);
    $(".myth_line").on('click', function() {
        var index=$(this).prop("title");
        console.log(index);
        batch_array.splice(index,1);
        $(this).animate({height: '0px', opacity: '0'}, "fast");
        setTimeout('draw_batch(batch_array)',200);
    });
}

function switch_display(part) {
    document.getElementById("part_1").setAttribute("style","display:none");
    document.getElementById("part_2").setAttribute("style","display:none");
    if(part==1){
        document.getElementById("part_1").setAttribute("style","display:block");
    }else if(part==2){
        document.getElementById("part_2").setAttribute("style","display:block; margin-top:100px");
    }

}

function generate_batch_inputs(batch_num){
    var workload=document.getElementById("workload").value;
    document.getElementById('batch_inputs').innerHTML = "";
    for(var i=0; i<batch_num; i++){
        var batch_input=document.createElement("input");
        batch_input.setAttribute("id","batch_input_"+i);
        batch_input.setAttribute("class", "form-control");
        batch_input.setAttribute("style","display:inline-block; border-color:#555555; margin-left:10px ; margin-bottom:10px; width:"+100+"px");
        if(i!=(batch_num-1)){
            batch_input.setAttribute("value",parseInt((workload/batch_num).toFixed(0)));
        }else{
            batch_input.setAttribute("value",workload-(batch_num-1)*parseInt((workload/batch_num).toFixed(0)));
        }

        document.getElementById('batch_inputs').append(batch_input);
    }
}

function plot_batch(batch_array){
    var dataset=document.getElementById("chdataset").value;
    var workload=parseInt(document.getElementById("workload").value);
    var algorithm=document.getElementById("chalg").value;
    var num_of_machines=document.getElementById("clusizeresult").value;
    var system=document.getElementById("chsys").value;
    var batch_num=document.getElementById("userbatchnumber").value;
    var memory=parseInt(document.getElementById("memory").value);
    var init_mem;
    if(dataset=="web"){
        init_mem=17793069/1024;
    }else if(dataset=="dblp"){
        init_mem=31678840/1024;
    }
    var param_local;
    if(system=="graphd"){
        param_local=param_json.giraph;
    }else if(system=="pregelplus"){
        param_local=param_json.pregelplus;
    }
    if(dataset=="web"){
        param_local=param_local.web_stanford;
    }else if(dataset=="dblp"){
        param_local=param_local.dblp;
    }
    console.log(param_local);
    if(algorithm=="ppr"){
        param_local=param_local.ppr[num_of_machines];
    }else if(algorithm=="sssp"){
        param_local=param_local.sssp[num_of_machines];
    }
    console.log(param_local);

    var layout={
        height:460,
        width:850,
        margin: {
            l: 55,
            r: 20,
            b: 34,
            t: 0,
            pad: 0
        },
        yaxis: {
            title: 'Estimated Memory Usage (GB)'
        },
        xaxis: {
            title: 'Workload Metric',
        },
        hovermode: false,
        annotations: [
            {
                x: workload*4/5,
                y: memory*900/1024,
                xref: 'x',
                yref: 'y',
                text: 'memory limitation',
                showarrow: false,
                font: {
                    family: 'Courier New, monospace',
                    size: 16,
                    color: '#ffffff'
                },
                align: 'top',
                bordercolor: '#c7c7c7',
                borderwidth: 2,
                borderpad: 4,
                bgcolor: 'rgb(240,120,135)',
                opacity: 0.6
            }
            ]
    };
    var data=new Array();
    for(var i=0; i<batch_array.length; i++) {
        var x_array=new Array();
        var y_array=new Array();
        var batch_line=batch_array[i];
        var sum=0;
        var y_sum=init_mem;
        var y_pre=init_mem;
        for(var j=0; j<batch_line.length; j++){
            var batch_size=batch_line[j];

            x_array.push(sum);
            x_array.push(sum+batch_size/2);
            y_array.push(Math.min(y_sum,y_pre)/1024);
            y_pre=y_sum+param_local.HWM[0]*Math.pow(batch_size,param_local.HWM[1])+param_local.HWM[2];
            y_array.push(y_pre/1024);
            y_sum=y_sum+param_local.RSS[0]*Math.pow(batch_size,param_local.RSS[1])+param_local.RSS[2];
            sum=sum+batch_line[j];
            console.log(x_array);
            console.log(y_array);
        }
        x_array.push(sum);
        y_array.push(y_sum/1024);
        var trace = {
            x: x_array,
            y: y_array,
            marker: {size: 12, opacity: 0.9, symbol: 'circle', color:color_table[i%color_table.length],"line": { "width": 2, color: "#999999"}},
            mode: 'lines+markers',
            line: {color:color_table[i%color_table.length], width: 3},
            showlegend: false,
            hovertemplate:
                "%{y:.2f}h",
            type: 'scatter'
        };
        data.push(trace);
    }
    var trace = {
        name: 'Memory limitation',
        x: [0,workload],
        y: [memory,memory],
        marker: {size: 12, opacity: 0.9, symbol: 'circle', color: 'rgb(240,120,135)'},
        mode: 'lines',
        line: {color: 'rgb(240,120,135)', width: 3, dash:'dot'},
        showlegend: false,
        hovertemplate:
            "%{y:.2f}h",
        type: 'scatter'
    };
    data.push(trace);
    Plotly.newPlot('plot_1', data, layout, {displayModeBar: false});

}

function add_batch() {
    console.log(param_json.pregelplus.web_stanford.sssp["1"].HWM[0]);
    var dataset=document.getElementById("chdataset").value;
    var workload=document.getElementById("workload").value;
    var algorithm=document.getElementById("chalg").value;
    var num_of_machines=document.getElementById("clusizeresult").value;
    var system=document.getElementById("chsys").value;
    var batch_num=document.getElementById("userbatchnumber").value;
    var memory=document.getElementById("memory").value;
    load_batch(workload,batch_num);
    draw_batch(batch_array);
}

function rockit(){

      //Delete the items in the table created by the last rockit
      if (rocks>0){
        $( "#table-body_pregelplus_tra tr" ).remove();
        $( "#table-body_pregelplus_eval tr" ).remove();
        $( "#table-body_graphd_tra tr" ).remove();
        $( "#table-body_graphd_eval tr" ).remove();
        i = 0;
        j = 0;
        init_timestamp = timestamp;
      }

        //show the loading icon for the first time
        var trx = $('<tr id="pending-row">' + '<td>' + '<img src="/static/images/load2.gif"/>' +  '</td><td>'  + '</td><td>' +  '</td><td>' +  '</td><td>' +  '</td><td>' + '</td></tr>');
        if(document.getElementById("chsys").value=="graphd"){
          $("#table-body_graphd_tra").append(trx);
          //$("#table-body_graphd_tra tr:first").append($('<img src="/static/images/load2.gif"/>'));

         }else if(document.getElementById("chsys").value=="pregelplus"){
          //$("#table-body_pregelplus_tra tr:first").append($('<img src="/static/images/load2.gif"/>'));
          $("#table-body_pregelplus_tra").append(trx);

         }


      var x = Math.floor(Math.random() * 1000000000);
      //post parameters to the server
     $.ajax({
       
      url:'http://localhost:80/start_task',
      async:false,//for the return value
      type:'POST',
      contentType: "application/json",
      data:JSON.stringify({
        "dataset":document.getElementById("chdataset").value,
        "workload":parseInt(document.getElementById("workload").value),
        "algorithm":document.getElementById("chalg").value,
        "num_of_machines":document.getElementById("clusizeresult").value,
        "system":document.getElementById("chsys").value,
        "uuid":x,
      }),
      
      success : function(data){
            
            if (data.result.status!="Okay"){
              alert(data.result.status);
              return;
            }
            
            var i=1;
            timestamp=Date.now()/1000;
            init_timestamp = timestamp;
            while(phase!="end"){
             // setTimeout(function(){function1(x)},1000*i);
             function1(x)
             i++;
            }
            $("#pending-row").remove();
          },

    })


}



function init_all() {
    console.log("init");
    var dataset=document.getElementById("chdataset").value;
    var workload=document.getElementById("workload").value;
    var algorithm=document.getElementById("chalg").value;
    var num_of_machines=document.getElementById("clusizeresult").value;
    var system=document.getElementById("chsys").value;
    var batch_num=document.getElementById("userbatchnumber").value;
    var memory=document.getElementById("memory").value;

    generate_batch_inputs(batch_num);
    $("#chdataset,#workload,#chalg,#clusizeresult,#chsys").on('change', function() {
        batch_array=[];
        draw_batch(batch_array);
    });


}

    function rockit(){
        //Delete the items in the table created by the last rockit
        if (rocks>0){
            $( "#table-body_pregelplus_tra tr" ).remove();
            $( "#table-body_pregelplus_eval tr" ).remove();
            $( "#table-body_graphd_tra tr" ).remove();
            $( "#table-body_graphd_eval tr" ).remove();
            i = 0;
            j = 0;
            init_timestamp = timestamp;
        }

        //show the loading icon for the first time
        var trx = $('<tr id="pending-row">' + '<td>' + '<img src="/static/images/load2.gif"/>' +  '</td><td>'  + '</td><td>' +  '</td><td>' +  '</td><td>' +  '</td><td>' + '</td></tr>');
        if(document.getElementById("chsys").value=="graphd"){
            $("#table-body_graphd_tra").append(trx);
            //$("#table-body_graphd_tra tr:first").append($('<img src="/static/images/load2.gif"/>'));

        }else if(document.getElementById("chsys").value=="pregelplus"){
            //$("#table-body_pregelplus_tra tr:first").append($('<img src="/static/images/load2.gif"/>'));
            $("#table-body_pregelplus_tra").append(trx);

        }


        x = Math.floor(Math.random() * 1000000000);
        //post parameters to the server
        $.ajax({

            url:'http://localhost:80/start_task',
            async:false,//for the return value
            type:'POST',
            contentType: "application/json",
            data:JSON.stringify({
                "dataset":document.getElementById("chdataset").value,
                "workload":parseInt(document.getElementById("workload").value),
                "algorithm":document.getElementById("chalg").value,
                "num_of_machines":document.getElementById("clusizeresult").value,
                "system":document.getElementById("chsys").value,
                "uuid":x,
            }),

            success : function(data){

                if (data.result.status!="Okay"){
                    alert(data.result.status);
                    return;
                }

                var i=1;
                timestamp=Date.now()/1000;
                init_timestamp = timestamp;
                while(phase!="end"){
                    // setTimeout(function(){function1(x)},1000*i);
                    function1(x)
                    i++;
                }
                $("#pending-row").remove();
            },

        })
        phase="not started";
        rocks++;//To count the number of the clicks on rockit; when rocks>=1, rock it has to clear the tables before appending new metric items for new settings.
    }


    window.addEventListener('load', (event) => {
        // $('#graphd12').focus();
//   if ("createEvent" in document) {
//     var evt = document.createEvent("HTMLEvents");
//     evt.initEvent("change", false, true);
//     $('#chsys').dispatchEvent(evt);
// }
// else
//     $('#chsys').fireEvent("onchange");
        $('#chdataset')[0].selectedIndex = 1;

        $('#chsys')[0].selectedIndex = 0;
        $('#chsys').trigger('onchange');

        $('#chalg')[0].selectedIndex = 0;


        $('#clusizeresult')[0].selectedIndex = 2;
        $('#clusizeresult').trigger('onchange');


    });