// using jQuery

$(document).ready(function() {

  spinner = $('#progress');
  $('#progress').css('visibility','hidden');

  $(".showhelp").hide();

  // hide all capacity status statistics
  $(".terrstats").hide();
  $(".midtwstats").hide();
  $(".logtwstats").hide();

  $(".showall").hide();

  $("[class^='dcstats_']").hide();
  $("[class^='midtwstats_']").hide();
  $("[class^='logtwstats_']").hide();

  $("#threshpopup").hide();
  $(".commtdetail").hide();

  $('.statsdetails, .statsinfo, .showhide, .capstatshelp, .terrlegend, .commenticon, .getdetails').hover(function() {

	//alert('hovering');
  	$(this).css('cursor','pointer');

 	}, function() {
	$(this).css('cursor','auto');
  });

//===============================================================================

  $("#showhelp").click( function() 
      {
          $('.showhelp').slideToggle('slow');
      });

  // COMMENTS
  //toggle the next component with class terrlegend
  $(".showcomment").live('click', function()
      {
	  var commentid = $(this).attr('commentid');
          $('.commtdetail_' + commentid).slideToggle('slow');
      });

  $(".revertcomment").live('click', function()
      {

	  var orig_value,
	      which = $(this).attr('which');

	  if ( which == 'title' ) {

	  	var whichid    = $(this).attr('whichid');
	  	orig_value = $('#orig_title_' + whichid).val();
	  	$('#commttitle_' + whichid).val(orig_value);
	  }
	  else {

		var tempid = $(this).attr('tempid'),
	  	    whichid    = ( tempid ) ? tempid : $(this).attr('whichid');
		
	  	orig_value = $('#orig_customer_' + whichid).val();
	  	$('#commtcustomer_' + whichid).val(orig_value);
	  	orig_value = $('#orig_throughput_' + whichid).val();
	  	$('#commtthroughput_' + whichid).val(orig_value);
	  }
      });

  $(".deletecomment").live('click', function()
      {

	var detailid    = $(this).attr('detailid');
	var commentid   = $(this).attr('commentid');
	var tempid   	= $(this).attr('tempid');

	// dealing with a newly added comment so cannot delete 
	// as not yet in the database
	if ( detailid.match(/^N/) ) { 
          	$("#commtdetail_" + detailid).remove();
		return; 
	};

        if (! confirm('Do you really wish to delete this comment?')) return;

       	$.ajax({
               type: "GET"
              ,url: "/capacity/delete_comment_detail/" + detailid
              ,dataType: "json"
              ,data: "data"
              ,cache: false
	      ,async: false
              ,success: function(data)
                        {
				var success  = data.success;

				if ( success == 1 ) {
					if ( tempid != '' ) {
						detailid  = tempid;
					}
          				$("#commtdetail_" + detailid).remove();
				}
				else {
					alert('Comment could not be deleted.');
				}

				// now adjust the total
				var throughput_sum = 0;
				$('.commtthroughput_' + commentid).each( 
					function (index, value) {
						if ($(this).val().match(/^\d+$/)) {
							throughput_sum += parseInt($(this).val());
						}
					});
				$('#throughput_sum_' + commentid).val(throughput_sum);

                        }
	      ,error: function(xhr, textStatus, errorThrown) {
	      	alert(textStatus); return false;
	      }
     	});
  });

  $(".savetitle").click(function()
      {

        if (! confirm('Do you really wish to save this title?')) return;

	var titleid = $(this).attr('titleid');
	var title   = $('#commttitle_' + titleid).val();

       	$.ajax({
               type: "GET"
              ,url: "/capacity/save_comment_title/" + titleid + '/' + title
              ,dataType: "json"
              ,data: "data"
              ,cache: false
	      ,async: false
              ,success: function(data)
                        {
				var success  = data.success;
				if ( success == 2 ) {
					var orig_value;
					alert('Title could not be saved.');
				  	orig_value = $('#orig_title_' + titleid).val();
				  	$('#commttitle_' + titleid).val(orig_value);
				}
				else {
				  	$('#orig_title_'   + titleid).val(title);
				}
                        }
	      ,error: function(xhr, textStatus, errorThrown) {
	      	alert(textStatus); return false;
	      }
     	});
  });

  $(".savedetail").live('click', function()
      {

	var tempid = $(this).attr('tempid');

	var detailid     = $(this).attr('detailid');
	var commentid    = $(this).attr('commentid');

	if ( tempid ) {
		var customer    = $('#commtcustomer_'   + tempid).val();
		var throughput  = $('#commtthroughput_' + tempid).val();
	}
	else {
		var customer    = $('#commtcustomer_'   + detailid).val();
		var throughput  = $('#commtthroughput_' + detailid).val();
	}

	if ( customer.match(/^\s*$/)  
		      || 
	     throughput.match(/^\s*$/) 
		      || 
	    !throughput.match(/^\-?\d+$/)) {
		alert('The comment cannot be empty and the seat allocation must be a number');
		return;
	}
	if ( throughput.match(/^\s*$/)) {
		throughput = 0;
	}

        if (! confirm('Do you really wish to save this comment?')) return;

       	$.ajax({
               type: "GET"
              ,url: "/capacity/save_comment_detail/" + commentid + '/' +  detailid + '/' + customer + '/' + throughput
              ,dataType: "json"
              ,data: "data"
              ,cache: false
	      ,async: false
              ,success: function(data)
                        {
				var success  = data.success;
				if ( success == 2 ) {
					var orig_value;
					alert('Comment could not be saved.');

					if ( tempid == '' ) {
					  	orig_value = $('#orig_customer_' + detailid).val();
					  	$('#commtcustomer_' + detailid).val(orig_value);
					  	orig_value = $('#orig_throughput_' + detailid).val();
					  	$('#commtthroughput_' + detailid).val(orig_value);
					}
				}
				else {

					// newly added comment detail
					if ( tempid != '' ) {
						detailid = tempid; 
						var validid = data.detailid;
					}
					else {
						var validid = detailid;
					}

					if ( tempid != '' ) {
						$('#commtdetail_' + detailid).children('.commenticon').each( 
							function (index, value) {
								if ( $(this).attr('detailid') ) {
										$(this).attr('detailid', validid);
								}
								if ( $(this).attr('whichid') ) {
										$(this).attr('whichid', validid);
								}
							});
				  		$(this).attr('detailid', validid);
					}

				  	$('#orig_customer_'  + detailid).val(customer);
				  	$('#orig_throughput_'+ detailid).val(throughput);

					// now adjust the total
					var throughput_sum = 0;
					$('.commtthroughput_' + commentid).each( 
						function (index, value) {
							throughput_sum += parseInt($(this).val());
						});
					$('#throughput_sum_' + commentid).val(throughput_sum);
				}
                        }
	      ,error: function(xhr, textStatus, errorThrown) {
	      	alert(textStatus); return false;
	      }
  	});
  });


  $("td.addcomment").click(function()
      {

	  var commentid, previous_detailtr, new_detailtr, new_detailtd;
	  var new_orig_customer, new_orig_throughput, new_detail_inp;

	  var date     = new Date();
	  var epoch    = date.getTime();
	  var randomid = 'N' + Math.floor(Math.random()*epoch );

	  assettype = $(this).attr('assettype');
	  anchorid  = $(this).attr('anchorid');
	  commentid = $(this).attr('commentid');
  	  $(".commtdetail_" + commentid).show();

          new_detailtr = $('<tr/>',
				{
				    	'class' : assettype + 'commt_' 	 + anchorid + 
						  ' commtdetail commtdetail_' + 
						   commentid,
				        'id'	: 'commtdetail_' + randomid
				});
	  new_detailtr.attr('detailid', randomid);

	  // hidden fields
          new_orig_customer = $('<input/>',
				{
				    	'class' : 'orig_customer',
					'type'  : 'hidden',
					'value' : '',
				});
	  new_orig_customer.attr('id',   'orig_customer_' + randomid);
	  new_orig_customer.appendTo(new_detailtr);

          new_orig_throughput = $('<input/>',
				{
				    	'class' : 'orig_throughput',
					'type'  : 'hidden',
					'value' : '',
				});
	  new_orig_throughput.attr('id',   'orig_throughput_' + randomid);
	  new_orig_throughput.appendTo(new_detailtr);

	  // 5 cells 
	  // 	ICONS
          new_detailtd = $('<td/>',
				{
				    	'class' : 'commenticon deletecomment',
				});
	  new_detailtd.attr('commentid', commentid);
	  new_detailtd.attr('detailid',  randomid);
	  new_detailtd.attr('tempid',    randomid);

	  $('<img/>', 
		  {
			'src'  : '/static/images/minus_15x15.png',
			'alt'  : 'Delete comment',
			'title': 'Delete comment'
		  }).appendTo(new_detailtd);

	  new_detailtd.appendTo(new_detailtr);

          new_detailtd = $('<td/>',
				{
				    	'class' : 'commenticon revertcomment',
				});
	  new_detailtd.attr('which',    'detail');
	  new_detailtd.attr('whichid',  randomid);
	  new_detailtd.attr('tempid',   randomid);

	  $('<img/>', 
		  {
			'src'  : '/static/images/undo_15x15.png',
			'alt'  :  'Revert comment',
			'title':  'Revert comment'
		  }).appendTo(new_detailtd);

	  new_detailtd.appendTo(new_detailtr);

          new_detailtd = $('<td/>',
				{
				    	'class' : 'commenticon savedetail'
				});
	  new_detailtd.attr('commentid', commentid);
	  new_detailtd.attr('detailid',  randomid);
	  new_detailtd.attr('tempid',    randomid);

	  $('<img/>', 
		  {
			'src'  : '/static/images/save_15x15.png',
			'alt'  : 'Save comment',
			'title': 'Save comment'
		  }).appendTo(new_detailtd);

	  new_detailtd.appendTo(new_detailtr);

	  // 	COMMENT: CUSTOMER, THROUGHPUT
          new_detailtd = $('<td/>',
				{
				    	'class' : 'customertd',
				});
	  new_detailtd.appendTo(new_detailtr);

          new_detail_inp = $('<input/>',
				{
				    	'class' : 'commtcustomer commtcustomer_' + commentid,
				    	'type'  : 'text',
				    	'value' : '',
				});
	  new_detail_inp.attr('id',  'commtcustomer_' + randomid);
	  new_detail_inp.attr('commentid', commentid);
	  new_detail_inp.attr('detailid',  randomid);
	  new_detail_inp.attr('maxlength',  100);
	  new_detail_inp.appendTo(new_detailtd);

          new_detailtd = $('<td/>',
				{
				    	'class' : 'throughputtd',
				});
	  new_detailtd.appendTo(new_detailtr);
          new_detail_inp = $('<input/>',
				{
				    	'class' : 'throughput commtthroughput_' + commentid,
				    	'type'  : 'text',
				    	'value' : '',
				});
	  new_detail_inp.attr('id',  'commtthroughput_' + randomid);
	  new_detail_inp.attr('commentid', commentid);
	  new_detail_inp.attr('detailid',  randomid);
	  new_detail_inp.attr('maxlength',  6);
	  new_detail_inp.appendTo(new_detailtd);

	  // append the row
          $(this).parent('tr.addcomment').before(new_detailtr);

      });

  //toggle the next component with class terrlegend
$(".showterr").click(function()
      {
          $(this).next(".terrstats").slideToggle('slow');
      });

// get logical tower detailed stats
$(".getdetails").click( _getstats_click_handler );

$(".statsdetails").click(function()
      {
	  var anchorid   = $(this).attr('anchorid');
	  var assettype = $(this).attr('assettype');

          var statsclass = "." + assettype + "stats_" + anchorid;

	  if ( $(statsclass).is(":visible") ) {
  	  	$(statsclass).hide('slow');
		$('.' + assettype + 'commt_' + anchorid ).hide();
	  }
	  else {
  	  	$(statsclass).show('slow');
		$('.' + assettype + 'commt_' + anchorid ).show();
	  }

		
});

$(".showthresh").live('click', function()
    { 
			    $(this).addClass('highlight_cell');
			    _show_threshold_popup( this ); 
    });

function _show_threshold_popup ( elem ) 
    {
	_setup_threshold_info( elem );
	$( "#threshpopup" ).dialog(
                              { buttons: { "Cancel": function() { 
			    				    $(this).removeClass('highlight_cell');
							    $(this).dialog("close"); } } 
                              }, 
			      { zIndex:   '300px',
				width:    '750px', 
				minWidth: '750px',
				hide:   { effect: 'drop', direction: "down" }
			      });
}

$("#threshold_form").ajaxForm(  
				{ 
        			     beforeSubmit:  showRequest   // pre-submit callback 
        			    ,success:       showResponse  // post-submit callback 
				    ,type: 	   'post'
				    ,dataType: 	   'json' 
				}
 	 		);

$("#thresh2def").change( function(){ 

	if ($('#thresh2def').is(':checked')) {
		$('#current_orange_peak').val($('#default_orange_peak').val());			
		$('#current_orange_95th').val($('#default_orange_95th').val());			
		$('#current_red_peak').val($('#default_red_peak').val());			
		$('#current_red_95th').val($('#default_red_95th').val());			

		$('#thresh2def').attr('checked', true);
		$('#thresh_set2default').val(1);			
	}
	else {
		$('#thresh2def').attr('checked', false);
		$('#thresh_set2default').val(0);			

	}

	return false;
    });

$(".thresh2set").change( function()
    { 
	$('#thresh_set2default').val(0);			
	$('#thresh2def').attr('checked', false);			
	return false;
    });


  $("[class^='midtwallstats_']").click(function()
      {
	  var anchor = $(this).attr('anchorid');
          
  	  $(".midtwstats_" + anchor).slideToggle('slow');
  	  $(".logtwstats_" + anchor).slideToggle('slow');
      });

  $("#toggleallterr").click(function()
      {
          $(".terrstats").slideToggle();
      });

  $("#showallterr").click(function()
      {
    	$('.logtwstats').hide();
    	$('.afwstats').hide();
  	$("[class^='dcstats_']").hide();

  	$(".dcdrill").hide();
  	$(".midtwdrill").hide();
  	$(".logtwdrill").hide();

    	$('.terrstats').show();
    	return false;
      });

  $("#showallcapstats").click(function()
      {
	if ($("#showallcapstats").html() == 'Expand All') {
    		$('.showall').show();
  		$("[class^='commt_']").show();

		$("#showallcapstats").html('Collapse All');
	}
	else {
    		$('.showall').hide();
  		$("[class^='commt_']").hide();
		$("#showallcapstats").html('Expand All');
	}
    	return false;
      });

  $("#hideallcapstats").click(function()
      {
    	$('.showall').hide();
    	return false;
      });

});	// end of  $(document).ready

// ============================================== FUNCTIONS ================================================ 

// shows/hides stats
function _anchorname_click_handler ( ) {
//--------------------------------
       	var anchortype = $(this).attr('anchortype');		// dc/midtw
       	var anchorid   = $(this).attr('anchorid');		// dcid/midtwid

        $('#' + anchortype + "_drill_" + anchorid).slideToggle('slow' );

}

// after a successful Ajax call, additional stats are added to the bandwidth metrics
// ---------------------------------------------------------------------------------
function _create_summary_content (container, data) {

       	var assettype  = data.assettype;	
       	var anchortype = data.anchortype_info[assettype].anchortype;	

	var assettypename = 'csw';
	if ( assettype == 5 ) {
		assettypename = 'sss';
	}
	else {
		assettypename = 'afw';
	}
	

	var metrictype = { 
			     0 : 'peak',
			     1 : '95th', 
			 }

	var table, rowc, row, cellc, cellh, cellv, cellc;

	// loop through all keys (ie parent anchors)
	// 	loop through all anchors (ie anchors: logtowers of midtowers)
	for ( var parent in data.stats_data ) {

		// loop through logtowers
		var anchors = data.stats_data[parent].anchors;

		for ( var i = 0; i < anchors.length; i++ ) {

			var all_stats   = anchors[i].stats.all;

			var anchorid    = anchors[i].anchorid;
			var anchorname  = anchors[i].anchorname;
			var anchormodel = anchors[i].anchormodel;

	//				alert(anchorname + ' - ' + all_stats.length);


			//var showhideclass = assettypename + 'stats_' + anchorid;
			var rowclasses = 'showall ' + anchortype    + 'stats_' + parent   + 
					 ' '        + assettypename + 'stats_' + anchorid;
 
			var colspan = data.period_headers.length + 2;
			rowv  = $('<tr/>',
				{
				    	'class' : 'dynamstats ' + rowclasses
				});
			cellh = $('<td/>', 	
				{
				    	'colspan' : colspan 
				});
			cellh.appendTo(row);
			rowv.insertAfter(container);

			container = rowv;

			//var all_stats = stats.all;
			for ( var stat_i = 0; stat_i < all_stats.length; stat_i++ ) {

				for ( var j in [0, 1] ) {

					var this_stat = all_stats[stat_i];
	
					var units      = data.assets_stats_info[assettype][stat_i].units;
					var multiply   = data.assets_stats_info[assettype][stat_i].multiply;
					var action     = data.assets_stats_info[assettype][stat_i].action;
					var metricid   = data.assets_stats_info[assettype][stat_i].metric_id;
					var metricid   = data.assets_stats_info[assettype][stat_i].metric_id;
					var metricname = data.assets_stats_info[assettype][stat_i].metric_name;
					var rowv  = $('<tr/>',
							{
							    	'class' : rowclasses
							});
					rowv.insertAfter(container);

					// incorrect identifier (in IE) - class must be quoted
					cellh = $('<td/>', 	
						{
						    	'class' :  'statsinfofield columnheader'
						});

					if ( j == 0 ) {
						cellh.html(metricname + '(<span class="units">' + units + 
									'</span> ' + action + ')' );
					}
					else {
						cellh = $('<td/>'); 	
					}
					cellh.appendTo(rowv);


					for ( var combo_i in this_stat ) {

						cell = $('<td/>', 	
						{
					    	    'class' : 'columnvalue statsinfo detailstats showthresh '
						});
						var title = metrictype[j] + ' value for ' + metricname +
							    ' for ' 	  + data.period_headers[combo_i];

						cell.attr( 'title', 	  title);
						cell.attr( 'assettypeid', assettype);
						cell.attr( 'metricid',    metricid);
						cell.attr( 'metrictype',  metrictype[j]);
						cell.attr( 'metricname',  metricname);
						cell.attr( 'anchorid',    anchorid);
						cell.attr( 'anchorname',  anchorname);
						cell.attr( 'anchormodel', anchormodel);
						cell.attr( 'periodname',  data.period_headers[combo_i]);
						
						var jj;
						if ( j == 0 ) { jj=2 }
						else	      { jj=3 }
						var threshclass  = 'threshcolour' + this_stat[combo_i][jj];
						var trafficclass = 'traffic_' + assettypename + '_' + anchorid;

						if (this_stat[combo_i][j] == 'N/A') { 
							var value4display = this_stat[combo_i][j];
						} 
						else {
							var value4display = this_stat[combo_i][j] * multiply;
							value4display = Math.round(value4display*100)/100;
						}
						cell.html(value4display);
						cell.appendTo(rowv);
						cell.addClass(threshclass);
						cell.addClass(trafficclass);

						cell.click( function () {
							_setup_threshold_info( cell );
							$( "#threshpopup" ).dialog({ buttons: 
								{ "Cancel": 
								     function() { 
									$('#thresh_set2default').val(0);
			    						$(this).removeClass('highlight_cell');
									$(this).dialog("close");
								} } }, 
							      	{ 
								   zIndex:   '300px',
								   width:    '750px', 
								   minWidth: '750px',
								   hide:   
									{ effect: 'drop', direction: "down" }
							      	});
						});
					}

					container =  rowv;
				}
			}
			cell = $('<td/>'); 	
			cell.appendTo(rowv);
		}
	}
	
}

function _getstats_click_handler ( ) {
//--------------------------------
//
       	var statstype  = $(this).attr('statstype');		// s/d/b
       	var assettype  = $(this).attr('assettype');		// sss/afw/csw
       	var anchortype = $(this).attr('anchortype');		// midtw/logtw
       	var anchorid   = $(this).attr('anchorid');		// $midtwid/$logtwid
       	var parentid   = $(this).attr('parentid');		// $midtwid/$logtwid

	var container_id  = '#' + assettype + 'detail_' + parentid + '_' + anchorid;

	var container = $(container_id);

	if ( container.next().hasClass('dynamstats') ) {
          	$("." + assettype + "stats_" + anchorid).slideToggle('slow');
	}
	else {
		spinnerDisplay(spinner, 'visible');
	       	$.ajax({
	               type: "GET"
	              ,url: "/capacity/base/get_stats/" + assettype  + '/' + statstype + '/' 
							+ anchortype + "/" + anchorid
	              ,dataType: "json"
	              ,data: "data"
	              ,cache: false
		      ,async: true
		      //,async: false
	              ,success: function(data)
	                        {
					var stats_data = data.stats_data;

	        			_create_summary_content(container, data);
          				$("." + assettype + "stats_" + anchorid).show('slow');

					spinnerDisplay(spinner, 'hidden');
	                        }
		      ,error: function(xhr, textStatus, errorThrown) {
		       //Error
		      	//alert(textStatus); return false;
		      }
	            });

	}

}

function _setup_threshold_info( stats_cell ) {

	//$('#thresh_set2default').val(0);			

	var thresh_assettypeid = $(stats_cell).attr('assettypeid');
	var thresh_anchorid    = $(stats_cell).attr('anchorid');
	var thresh_anchorname  = $(stats_cell).attr('anchorname');
	var thresh_anchormodel = $(stats_cell).attr('anchormodel');
	var thresh_metricid    = $(stats_cell).attr('metricid');
	var thresh_metrictype  = $(stats_cell).attr('metrictype');
	var thresh_metricname  = $(stats_cell).attr('metricname');
	var thresh_periodname  = $(stats_cell).attr('periodname');

	thresh_periodname = thresh_periodname.replace(/\-\d+/, '');

	var thresh_assettype;
	if (thresh_assettypeid == 3) {
		thresh_assettype = 'CORE SWITCH in ';
	}
	else if (thresh_assettypeid == 4) {
		thresh_assettype = 'FIREWALL in ';
	}
	else {
		thresh_assettype = 'LOGICAL TOWER  ';
	}


	$('#thresh_assettypeid').val(thresh_assettypeid);	
	$('#thresh_anchorid').val(thresh_anchorid);	
	$('#thresh_anchormodel').val(thresh_anchormodel);	
	$('#thresh_metricid').val(thresh_metricid);	
	$('#thresh_metrictype').val(thresh_metrictype);	
	$('#thresh_periodname').val(thresh_periodname);	

	// ajax call to get the current threshold and threshold default 
       	$.ajax({
               type: "GET"
              ,url: "/capacity/get_thresholds/" + thresh_assettypeid  + '/' + thresh_anchorid   + 
					   '/' + thresh_anchormodel   +  
					   '/' + thresh_metricid      + '/' + thresh_periodname
              ,dataType: "json"
              ,data: "data"
              ,cache: false
	      ,async: false
              ,success: function(data)
                        {

				var modelname    = data.modelname;
				var set2default  = data.set2default;

				var thresh_title = 'Threshold Data for '  + thresh_assettype  + ' ' + 
						    thresh_anchorname  	  + 
						   '<br />Metric: '       + thresh_metricname + 
						   '<br />Metric type: '  + thresh_metrictype  +  
			   			   '<br />Period type: '  + thresh_periodname + 
						   '<br />Model: ' 	  + modelname;

				$('#thresh_title').html(thresh_title);	

				$('#thresh2def_info').html(set2default_title);	

				var current_orange_peak = data.current_threshold['orange']['peak'];
				var current_orange_95th = data.current_threshold['orange']['95th'];
				var current_red_peak 	= data.current_threshold['red']['peak'];
				var current_red_95th 	= data.current_threshold['red']['95th'];

				var default_orange_peak = data.default_threshold['orange']['peak'];
				var default_orange_95th = data.default_threshold['orange']['95th'];
				var default_red_peak 	= data.default_threshold['red']['peak'];
				var default_red_95th 	= data.default_threshold['red']['95th'];

				if (set2default == 1) {
					$('#thresh2def').attr('checked', true);	
					var is_or_not_updated = 'Automatically updated';
				}
				else {
					$('#thresh2def').attr('checked', false);	
					var is_or_not_updated = 'Not automatically updated';
				}
				var set2default_title = is_or_not_updated + ' to default thresholds';

				$('#thresh_set2default').val(set2default);	

				$('#current_orange_peak').val(current_orange_peak);	
				$('#current_orange_95th').val(current_orange_95th);	
				$('#current_red_peak').val(current_red_peak);	
				$('#current_red_95th').val(current_red_95th);	

				$('#default_orange_peak').val(default_orange_peak);	
				$('#default_orange_95th').val(default_orange_95th);	
				$('#default_red_peak').val(default_red_peak);	
				$('#default_red_95th').val(default_red_95th);	

				$('#orig_current_orange_peak').val(current_orange_peak);	
				$('#orig_current_orange_95th').val(current_orange_95th);	
				$('#orig_current_red_peak').val(current_red_peak);	
				$('#orig_current_red_95th').val(current_red_95th);	

				$('#orig_default_orange_peak').val(default_orange_peak);	
				$('#orig_default_orange_95th').val(default_orange_95th);	
				$('#orig_default_red_peak').val(default_red_peak);	
				$('#orig_default_red_95th').val(default_red_95th);	

                        }
	      ,error: function(xhr, textStatus, errorThrown) {
	       //Error
	      	alert(textStatus); return false;
	      }
            });

}

function showRequest (formData, jqForm, options) {
}

function showResponse (data, statusText, xhr, $form) {

/*
 *
 * {"red_95th":"320","periodname":"Month","metricid":"1","orange_95th":"221","outcome":1,"assettypeid":"3","red_peak":"220","orange_peak":"218","metrictype":"peak","anchormodel":"467","anchorid":"43"}
 *
traffic_csw_$anchorid_$metric_id ... traffic_1111_3
 *
 */

	// alert if outcome == 2 => error
	if ( data.outcome == 2 ) {
		alert('There was a problem updating some of the thresholds. Please check that the values are numbers. If these are of valid format, please, contact the CMDB team.');
		return;
	}

	if ( data.assettypeid == 3 ) {
		var assettype = 'csw';
	}
	else if ( data.assettypeid == 4 ) {
		var assettype = 'afw';
	}
	else {
		var assettype = 'sss';
	}

	var orange = data.orange;
	var red    = data.red;

	//var periodname = data.periodname;
	var cell_class_array = _create_traffic_selector (assettype, 	data.anchorid, 
							 data.metricid, data.periodname);
	// updates traffic lights of the particular element
	_update_traffic_lights( orange, red, cell_class_array );
	
	if ( data.thresh_def_changed == 1 ) {
	
		// loop through anchors that need the traffic lights changed because their thresholds 
		// were set to updated defaults
		var elems2update = data.anchorids;
		$.each( elems2update, function ( index, id ) { 
			var cell_class_array = _create_traffic_selector (assettype, 	id, 
							 		 data.metricid, data.periodname);
	 		// updates traffic lights of the particular element
			_update_traffic_lights( orange, red, cell_class_array );
		});
	}

	$('#thresh_set2default').val(0);
	$('#threshpopup').dialog("close");
}



function _remove_thresh_classes ( cell ) {

	if ( cell.hasClass('threshcolour0') ) {
		cell.removeClass('threshcolour0')
	}
	else if ( cell.hasClass('threshcolour1') ) {
		cell.removeClass('threshcolour1')
	}
	else if ( cell.hasClass('threshcolour2') ) {
		cell.removeClass('threshcolour2')
	}
	else if ( cell.hasClass('threshcolour3') ) {
		cell.removeClass('threshcolour3')
	}
	else if ( cell.hasClass('threshcolour4') ) {
		cell.removeClass('threshcolour4')
	}
}

function _create_traffic_selector (assettype, anchorid, metricid, periodname) {

	// now we select the right metric to work on
	var cell_class_array = [];
	var cell_class = '.traffic_' + assettype + '_' +  anchorid;
	var metric_selector = [];
	metric_selector[0] = '[metricid="1"][periodname^="'+ periodname  + '"]';
	metric_selector[1] = '[metricid="2"][periodname^="'+ periodname  + '"]';
	metric_selector[2] = '[metricid="' + metricid + '"][periodname^="' + periodname  + '"]';

	if ( metricid == 1 || metricid == 2 ) {
		cell_class_array[0] = cell_class + metric_selector[0];
		cell_class_array[1] = cell_class + metric_selector[1];
	}
	else {
		cell_class_array[0] = cell_class + metric_selector[2];
	}

	return cell_class_array;
}


function _update_traffic_lights ( orange, red, cell_class_array ) {

	for ( var i=0; i<cell_class_array.length; i++) {

		var this_class = cell_class_array[i];
		$.each( $(this_class), function ( j, elem ) {
					
			    		$(this).removeClass('highlight_cell');

					var metrictype = $(elem).attr('metrictype');
					var elem_value = $(elem).html();
					if ( elem_value.toString() == 'N/A' ) {
						var isNA = 1;
					}
					else {
						elem_value = parseFloat(elem_value);
						var isNA = 0;
					}
					var elem_class = $(elem).attr('class');

					var orange_max = orange[metrictype];
					var red_max    = red[metrictype];

					_remove_thresh_classes($(elem));

					if ( elem_value.toString() == 'N/A' || elem_value == 'N/A' ) { 
						$(elem).addClass('threshcolour0');
						//alert('added threshcolour0 to ' + elem_value + '(' + j + ')');
					}
					else if ( 
						orange_max == 'Not set' || 
						red_max    == 'Not set' || 
						typeof(orange_max) == 'undefined' || 
					        typeof(red_max) == 'undefined' ) 
					{ 
						$(elem).addClass('threshcolour4');
						//alert('added threshcolour4 to ' + elem_value + '(' + j + ')');
					}
					else if ( elem_value < orange_max ) {
						//alert('added threshcolour1 to ' + elem_value  + '(' + j + ')');
						$(elem).addClass('threshcolour1');
					}
					else if ( elem_value >= orange_max && elem_value < red_max ) {
						//alert('added threshcolour2 to ' + elem_value  + '(' + j + ')');
						$(elem).addClass('threshcolour2');
					}
					else if ( elem_value >= red_max ) {
						//alert('added threshcolour3 to ' + elem_value  + '(' + j + ')');
						$(elem).addClass('threshcolour3');
					}
	
			  	 } );
	}
}

function createSpinner (message) {
	var spinner_container = $('<div/>',
				    {
				'class': 'spinner_container',
				'id':    'progress'
				    }
			);
	var spinner_message = $('<div/>',
				    {
				'class': 'spinner_message',
				'id':    'message'
				    }
			);
	spinner_message.appendTo($(spinner_container));
	spinner_message.html(message);
	var image = $('<img/>',
			{
			    'class': 'spinner_image',
			    'src': '/static/images/progress.gif',
			});
	image.appendTo($(spinner_container));
	spinner_container.appendTo($('body'));

	spinner_container.css('visibility','hidden');

	return spinner_container; 
}

function spinnerDisplay (spinner, mode) {
	$(spinner).css('visibility', mode);
}

