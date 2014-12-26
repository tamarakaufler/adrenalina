$(function() {
      if ( $("#query").length > 0 ) {

        $( "#query" ).autocomplete({
                  source: function( request, response ) {
                          $.ajax({
                                  url: '/staport/checkcompany',
                             dataType: 'jsonp',
                                 data: {
                                         term: request.term
                                       },
                              success: function(data) {
                                           response( $.map( data.company, function( item ) {
                                                     return  {
                                                         label: item,
                                                         value: item
                                                     }
                                           }));
                                       }
                                 });
                          },
                   delay: 0,
               minLength: 2,
                dataType: 'json',
                     max: 20
        });

      }
});

$(document).ready(function(){

    $("legend.datacentre_name").click(function() {
        var ident = $(this).attr('ident');

        var id = '#datacentre_' + ident + "_container";
        if ($(id).is(":visible")) {
            $(id).fadeOut();
        }
        else {
            $(id).fadeIn(1000);
        }
        
    });

    $(".anchor").click(function() {
        var anchor = $(this).attr('anchor');
        var ident  = $(this).attr('ident');
        var anchor_class  = anchor + "_" + ident + "_stats";

        if ($('.' + anchor_class).is(":visible")) {
            $('.' + anchor_class).fadeOut();
        }
        else {
            $('.' + anchor_class).fadeIn();
        }
    });

    $(".stat_cell").click(function() {
        $("#graph_popup").hide();
        $(".graphite_imgs").hide();

        var anchor = $(this).attr('anchor');
        var ident  = $(this).attr('ident');
        var graph  = $(this).attr('graph');

        // show the graph popup    
        var winWidth  = $(window).width();
            var winHeight = $(window).height();

        var rand_img = "/static/images/Graph" + Math.floor((Math.random()*5)+1) + ".png";
        var image = $("<img id='graph_img_popup' class='graphite_imgs' src='"  + rand_img + "'>");

        $("#graph_container").append(image);
            $("#graph_popup").css("position","absolute")
                 .css("left", ((winWidth / 2) - ($("#graph_popup").width() / 2)) + "px")
                 .css("top", ($(window).scrollTop() + 150) + "px")
                 ;

        $("#graph_popup").show().draggable();
    });

    $("#graph_popup_cancel").click(function() {
        $("#graph_popup").hide();
    });

});

