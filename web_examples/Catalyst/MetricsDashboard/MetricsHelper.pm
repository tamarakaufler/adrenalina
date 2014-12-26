package Dashboard::Controller::Helpers::MetricsHelper;

=head1 Dashboard::Controller::Helpers::MetricsHelper

Helper methods used by the Metrics.pm controller

=cut

use v5.10;

#$ENV{ DBIC_TRACE } = 1;

use FindBin       qw($Bin);
use lib           "$FindBin::Bin/../../..";
use strict;
use warnings;

use Exporter;
use Time::Local;

use Data::Dumper qw(Dumper);
use DateTime;
use Data::Serializer;

use ASSETSConfig      qw(returnConfig);
use Dashboard::Controller::Helpers::AssetHelper    
        qw(
            get_logical_towersByParent
            get_towers
            get_firewalls
        );


our (@ISA)     = qw(Exporter);
our @EXPORT_OK  = qw(
            get_base_info
            get_datacentres
            get_stats_info
            get_period_info
            get_stats4asset_type
            retrieve_thresholds
            get_table_info
            update_thresholds
            do_thresh_update
            set_thresh2default
            is_valid_thresh_input
            cleanup_spaces

            destroy_comment_detail
            update_comment
            change_model4capacity
            update_capacity_thresholds_model
            );

our $config    = returnConfig();
our $cachetime = $config->{ CACHE }{ time4capstat };

=head3 get_stats_info

gets primary and secondary stats ids for each asset type 2/4/5

IN:    Catalyst object or schema
OUT:    hashref ... { 
              3 => [ 
                 {     metric_name => ...,
                metric_id   => ...,
                units        => ...,
                action        => ...,
                multiply    => ...,
                 },
                 {     metric_name => ...,
                metric_id   => ...,
                units        => ...,
                action        => ...,
                multiply    => ...,
                 },
               ],
              4 => [
                 {     metric_name => ...,
                metric_id   => ...,
                units        => ...,
                action        => ...,
                multiply    => ...,
                 },
                 {     metric_name => ...,
                metric_id   => ...,
                units        => ...,
                action        => ...,
                multiply    => ...,
                 },
               ],
              5 => [ ... ]
            }    

=cut

sub get_stats_info {
    my ( $c ) = @_;

    my ( $table_access, @metrics );
    if ( ref( $c ) eq 'MetricsStatusDB') {
        $table_access = $c->resultset('MetricsStatusRrdInfo');
    } 
    else {
        $table_access = $c->model('MetricsStatusDB::MetricsStatusRrdInfo');
    }

    @metrics = $table_access->search( {},
                                { 
                   join     =>   'metric_type',
                   select   => [ 'me.asset_type',    'me.metric',   'metric_type.name', 
                                 'me.action',        'me.units',    'me.multiply', ],
                   as        => [ 'asset_type',      'metric_id',   'metric_name',
                                  'action',          'units',       'multiply' ],
                   order_by => [ 'me.metric' ],
                   distinct => 1,
                                } 
                              )->all;

    my $assets_stats;
    foreach my $metric ( @metrics ) {
        my $asset_type  = $metric->get_column('asset_type');
        my $metric_id   = $metric->get_column('metric_id');
        my $metric_name = $metric->get_column('metric_name');
        my $units       = $metric->get_column('units');
        my $action      = $metric->get_column('action');
        my $multiply    = $metric->get_column('multiply');

        push @{ $assets_stats->{ $asset_type } }, { metric_name => $metric_name,
                                metric_id   => $metric_id,
                                units       => $units,
                                action      => $action,
                                multiply    => $multiply,
                              };
    }

    return $assets_stats;
}    

=head2 get_stats4asset_type 

The main method 

    ## core switches - gets stats and territory/dfatacentre info to populate the main page
    ## -------------
    
    ## firewalls/logical towers - used by Ajax calls to gather stats for a particular 
    ## datacentre/lidtower/logical tower
    ## ------------------------


IN: $c
    DateTime object
    stats_data .....  stats_type ... d (detail) / s (summary) / b (both)
              stats_info ... stats info for each asset type
    $period_info
    $asset_type ..... 3/4/5

    optional:
        $anchor_value:
                datacentre id/midtower id/logical tower id
                arrayref of datacentre ids/midtower ids/logical tower ids
        $is_parent:
                indicate whether anchor_value refers to parent/s or not

OUT:    arrayref for territories with arrayrefs for datacentres with hashrefs

=cut

sub get_stats4asset_type ($$$$$;$$) {
    my ( $c,       $dt,
         $stats_data, $period_info,  
         $asset_type, 
         $anchor_value, $is_parent ) = @_;

    my ( $stats_type, $stats_info )  = ( $stats_data->{ stats_type }{ $asset_type }, 
                         $stats_data->{ stats_info }{ $asset_type } );

    my $stats_ids      = [ map { $_->{ metric_id } } @{$stats_info} ];
    my $stats_multiply = [ map { $_->{ multiply }  } @{$stats_info} ];

    my $anchors    =  _get_anchors( $c, $asset_type, $anchor_value, $is_parent );

    ## gets thresholds for all the anchors
    my $threshold =  _get_thresholds( $c, $asset_type, $anchors );

    my $all_periods_search = _get_period_search ( $period_info, $dt );
    my $period_helper = {   period_info         => $period_info,
                            all_periods_search  => $all_periods_search }; 
    
    ## the core switch stats are all retrieved from the database when the Metrics status page is loaded
    my $stats2display = {};
    $stats_data = {
                    stats_type     => $stats_type,
                    stats_ids      => $stats_ids,
                    stats_multiply => $stats_multiply,
                  };

    $stats2display =  _get_stats( 
                      $c, $dt, 
                      $period_helper,
                      $asset_type,
                      $stats_data, 
                      $anchors,
                      $threshold );

    return $stats2display;

}

##===========================================================================================================

=head2 PRIVATE METHODS

=head3 _stats_ids_and_titles4asset_type

IN:    $asset_stats ... href { 3 => [ 1,2,3 ],
                4 => [ 1,2,3,4,5 ],    
                5 => [ 1,2,3,4,5, ... 29 ],    
        $asset_type  ... 3 (core switch/datacentre) / 4 (firewall/midtower) / 5 (logtower/sum of its scanning servers)

OUT:    array of arrayref of metric ids and arrayref of titles, that should be displayed for a device type

=cut

sub _stats_ids_and_titles4asset_type {
    my ( $asset_stats, $asset_type ) = @_;

    my @stats_ids    = ();
    my @stats_titles = ();
    foreach my $stats_href ( @{$asset_stats->{ $asset_type }} ) {
        push @stats_ids,    $stats_href->{ metric_id };
        push @stats_titles, $stats_href->{ metric_name };
    }

    return ( \@stats_ids, \@stats_titles );

}

=head3 _get_stats

populates the $anchors href framework with the stats data

IN:    $c, 
    DateTime object
    $stats_type ... d (detail) / s (summary) / b (both)
    $period_info,
    $asset_type,
    $stats_ids, 
    $anchors
    $threshold href for given anchors

OUT:    for core switches: 
    -----------------
    $datacentre_stats = 
        {   $terrname => 
            {   #     $terrname  for core switches,
                #     $dcname    for firewalls,
                #     $midtwname for logtowers
               childassettypeid   => 3,
               anchorids     => 
               anchornames   => 
               anchors       => 
                 [ 
                     {
                    anchorid   => $dcid, #    ($midtwid for firewalls, 
                                #     $logtwid for logtowers)
                    anchorname => $dcname,
                    stats =>
                          {
                              summary => 
                              [ 
                                [ max_value_peak, max_value_95th ],
                                [ max_value_peak, max_value_95th ],
                                   ...
                              ],
                              all     =>
                              [
                                [ value_peak, value_95th ], 
                                [ value_peak, value_95th ], 
                                     [ value_peak, value_95th ], 
                                ... 
                                   ],
                                 [
                                [ value_peak, value_95th ], 
                                [ value_peak, value_95th ], 
                                     [ value_peak, value_95th ], 
                                ... 
                                   ],
                          },

                    childassettypeid   => 4,
                    anchorids   => ...,
                    anchornames => ...,
                    anchors     => ...,
                             
                     },
                     ....
                  ]
            },
            {
                ...
            },
        }


=cut

sub _get_stats ($$$$$$$) {
    my ( $c, $dt, 
         $period_helper, 
         $asset_type,
         $stats_data, $owner_info, $threshold ) = @_;

    my $period_info        = $period_helper->{ period_info };
    my $all_periods_search = $period_helper->{ all_periods_search };

    ## determine if we want summary stats, detailed stats or both
    my $stats_type     = $stats_data->{ stats_type };
    my $stats_ids      = $stats_data->{ stats_ids };
    my $stats_multiply = $stats_data->{ stats_multiply };

##===================================================================
## FOR DEBUGGING
###=============
#$c->cache->set('capstatsdata', undef);
##===================================================================

    my $serializer = Data::Serializer->new();

    my $cacheinfo = $c->cache->get('capstatsdata');
    if ( $stats_type ne 'd' ) {
        if ( $cacheinfo ) {
            $owner_info = $serializer->deserialize( $cacheinfo );
            return $owner_info;
        }
    } 

    ## metric search criteria
    my $metric_search;
    $metric_search->{ '-or' } = [ 
                    { metric => 1 },
                          { metric => 2 } 
                    ] if $stats_type eq 's';

    my ( $dc_stats, $period_search, $anchor_search );

    ## loop through day/week/month periods
    my $period_index    = 0;    ## index of the displayed period selections

    ##================================== the main loop ==========================================
    foreach my $period_type ( @$period_info ) {

        my ($db_data, $period_data) = _create_dbic_search_options(
                                        $asset_type,
                                        $period_type,
                                        $all_periods_search,
                                     );
        my ($table,  $anchor,
            $period, $period_search, 
            $order_by )     = ( $db_data->{ table },      $db_data->{ anchor },
                                $period_data->{ period }, $period_data->{ period_search },
                                $period_data->{ order_by } );

        my $period_selections = $period_search->{ '-or' };
        my ( $per_ident_0, $per_ident_1 ) = ( $order_by->{ period }->[0], 
                                              $order_by->{ period }->[1] );  

        ## loop through territory names
        ##=============================
        foreach my $owner ( keys %$owner_info ) {
    
            my $anchorids  = $owner_info->{ $owner }{ anchorids };
            $anchor_search = _get_anchor_search ( $anchor, $anchorids );
    
            ## get DBIC stats results
            my $rows = _get_raw_stats($c,
                                $table, $anchor_search, 
                                $period_search, $period_data->{ order_by },
                                $metric_search, $asset_type);

            ## loop through the datacentres results and organize them for use in the template
            ##=====================================
            my $anchor_i = 0;
            foreach my $anchorid ( @$anchorids ) {

                my $stats_i = 0;
                ## loop through the metrics
                foreach my $stats_id ( @$stats_ids ) {

                    ## determines position in the selected periods
                    my ($sel_i, $sel_start);
                    $sel_i = $sel_start = $period_index;

                    my $sel_end   = $sel_start + @$period_selections - 1;
                    if ( not scalar @$rows ) {
                        for my $i ( $sel_start .. $sel_end ) {
                            $owner_info->{ $owner }{ anchors }[$anchor_i]
                                         { stats }{ all }[$stats_i][$i] = [ ( 'N/A', 'N/A', 0, 0 ) ] 
                        }
                        
                    }
                    else {
                        foreach my $period_sel ( @$period_selections ) {
                            my ( $value_peak, $value_95th ) = ('N/A', 'N/A');
    
                            foreach my $row ( @$rows ) {
                                if ( $anchorid == $row->$anchor &&
                                          $stats_id == $row->metric ) {
    
                                    ## see if stats for this period interval or stored
                                    if ( $period_sel->{ $per_ident_0 } == $row->$per_ident_0
                                                            &&
                                         $period_sel->{ $per_ident_1 } == $row->$per_ident_1 ) {
                                        ( $value_peak, $value_95th ) = ( $row->value_peak, $row->value_95th );
                                        last;
                                    }
    
                                }
    
                            }
                            ## set up the traffic light
                            ##    how do we decide about the colours
                            my $colour = 
                            _set_up_traffic_light(
                                $threshold->{$anchorid}{$stats_id}{$period_type->{ name }},
                                             $anchorid, $stats_id, 
                                             $period_type->{ name },
                                             $value_peak, $value_95th,
                                             $stats_multiply->[$stats_i]);
    
                            $owner_info->{ $owner }{ anchors }
                                  [$anchor_i]{ stats }{ all }[$stats_i]
                                  [$sel_i] =
                                  [ ($value_peak,  $value_95th,
                                 $colour->[0], $colour->[1]
                                ) ];
    
                            $sel_i++;
                        }
                    }
                    $stats_i++;
                }
                $anchor_i++;
            }
            _populate_summary_stats( $period_info, $owner_info->{ $owner }{ anchors } ) unless $stats_type eq 'd';
        }
        $period_index = $period_index + scalar @$period_selections;
    }
    ##================================== the main loop ==========================================

    ## we set up the cache if need be
    ##-------------------------------
    if ( $stats_type ne 'd' ) {
        if ( ! $c->cache->get('capstatsdata') ) {
                   $c->cache->set('capstatsdata', 
                       $serializer->serialize($owner_info), $cachetime );
        }
    }

    return $owner_info;
}

=head3 _get_anchor_search

set up DBIC anchor search href

=cut

sub  _get_anchor_search {
    my ( $anchor, $anchorids ) = @_;
    
    my @anchor_search = map { { $anchor => $_ } } @$anchorids;
    return { '-or' => \@anchor_search };
}

=head3 _set_up_traffic_light

IN:     $threshold
        $anchorid, $stats_id, $period_type,
        $value_peak, $value_95th
        $multiply (for this metric)
OUT:    arrayref of 2 numbers between 1 and 3:     1 ... green
                        2 ... orange
                        3 ... red

                        4 ... missing threshold

=cut

sub _set_up_traffic_light {
    my ($threshold, 
        $anchorid,   $stats_id,   $period_type,
        $value_peak, $value_95th,
        $multiply ) = @_;

    my $traffic_light;
    if ( ! $threshold ) {
        $traffic_light->[0] = ( $value_peak =~ m#N/A|Not# ) ? 0 : 4;
        $traffic_light->[1] = ( $value_95th =~ m#N/A|Not# ) ? 0 : 4;

        return $traffic_light;

    } 

    my $orange_thresh_peak     = $threshold->[0]; 
    my $orange_thresh_95th     = $threshold->[1];
    my $red_thresh_peak     = $threshold->[2];
    my $red_thresh_95th     = $threshold->[3];

    $traffic_light =
        _fire_up_traffic_lights({ orange_thresh   => {  peak   => $orange_thresh_peak,
                                                        '95th' => $orange_thresh_95th },
                                  red_thresh      => {  peak   => $red_thresh_peak,
                                                        '95th' => $red_thresh_95th }},
                                { peak  => $value_peak,
                                 '95th' => $value_95th },
                                 $multiply);

    return $traffic_light;
}

=head3 _fire_up_traffic_lights

changes $traffic_light arrayref values based on given metric values and given thresholds

IN:     $threshold ... orange/red threshold values for anchor id/metric id and period type (day/week/month)
        $value_href
        $multiply
OUT:    traffic lights arayref [ 0-4, 0-4 ]
                  -------   
                     |         
                 refers to      
                 orange        
                 peak and      
                 95th          
                            
=cut

sub _fire_up_traffic_lights {
    my ( $threshold, $value_href,
         $multiply ) = @_;

    my $traffic_light;

    my ( $orange_thresh_peak, $orange_thresh_95th ) = ( $threshold->{ orange_thresh }{ peak },
                                                        $threshold->{ orange_thresh }{ '95th' } );
    my ( $red_thresh_peak, $red_thresh_95th )       = ( $threshold->{ red_thresh }{ peak },
                                                        $threshold->{ red_thresh }{ '95th' } );
    my ( $value_peak, $value_95th ) = ( $value_href->{ peak }, $value_href->{ '95th' } );

    ## for peak
    $traffic_light->[0] = _set_peak_95th_red_orange( $orange_thresh_peak, $red_thresh_peak, 
                                                     $value_peak,         $multiply );

    ## for 95th
    $traffic_light->[1] = _set_peak_95th_red_orange( $orange_thresh_95th, $red_thresh_95th,
                                                     $value_95th,         $multiply );

    return $traffic_light;
}

=head3 _set_peak_95th_red_orange

IN:    $orange_thresh, $red_thresh, $value
OUT:    0-4 ... 0 ... if metric value N/A (was not collected)
        1 ... green
        2 ... orange
        3 ... red
        4 ... threshold info missing

=cut

sub _set_peak_95th_red_orange {
    my ($orange_thresh, $red_thresh, 
        $value, $multiply) = @_;

    my $light;

    if ( ! $value || $value =~ m#N/A# ) {
        $light = 0;
    }
    elsif ( not $orange_thresh || not $red_thresh  ) {
        $light = 4;
    }
    else {
        if (     $multiply * $value >= $red_thresh ) {
            $light = 3;
        }
        elsif ( $multiply * $value >= $orange_thresh ) {
            $light = 2;
        }
        else {
            $light = 1;
        }
    }

    return $light;
}

=head3 _create_dbic_search_options

=cut

sub _create_dbic_search_options ($$$) { 
    my ($asset_type, $period_type, $all_periods_search) = @_;
    
    my ($db_data, $period_data);

    my $period        = $period_type->{ name };
    my $period_search = $all_periods_search->{ $period };
    my @order_by_period = sort { $b cmp $a } keys %{ $all_periods_search->{ $period }{ '-or' }[ 0 ] }; 

    my ( $table, $anchor ) = _get_metric_search_info( $asset_type, $period );

    ( $db_data->{ table },      
      $db_data->{ anchor },
      $period_data->{ period }, 
      $period_data->{ period_search },
      $period_data->{ order_by }{ period } ) = ( $table, $anchor, $period, $period_search );

    ( $period_data->{ order_by }{ period }, 
      $period_data->{ order_by }{ anchor_metric } ) =
                           ( \@order_by_period, [( $anchor, 'metric' )] );

    return  ($db_data, $period_data);
}

=head3 _get_raw_stats

does the database query for one or more anchors (datacentre/midtower/logtower)

IN:    
    Catalyst object
    $anchor_search
    $table
    $anchor
    $period_search
    $order_by
OUT:    arrayref of DBIC objects

=cut

sub _get_raw_stats ($$$$$;$$) {

    my ( $c, 
         $table, $anchor_search, 
         $period_search, $order_by,
         $metric_search, $asset_type ) = @_;

    $order_by = [ { '-asc'  => $order_by->{ anchor_metric } },
                  { '-desc' => $order_by->{ period } } ];

    my $and_search_aref = [ $anchor_search, $period_search, ];
    push @$and_search_aref, $metric_search if $metric_search;

    my @rows = ();
    @rows = $c->model( "MetricsStatusDB::$table" )->search( 
                        { 
                          '-and'     => $and_search_aref,
                        },
                              {  
                          'order_by' => $order_by,
                              } );

    return \@rows;
}

=head3 _populate_summary_stats

creates the array for summary display
updates the anchors stats info with the summary values

IN:    $period_info
    $anchors_ref
OUT:    N/A

=cut

sub _populate_summary_stats {

    my ( $period_info, $anchors_ref ) = @_;

    my @anchors = @$anchors_ref;

    my $period_count = 0;
    foreach my $period_type ( @$period_info ) {
        $period_count += scalar @{$period_type->{ decrements }};
    }
    my $period_max_index = $period_count - 1;

    foreach my $anchor ( @anchors ) {
    ## ----------------------------

        my $values_combo_stats1 = $anchor->{ stats }{ all }[0];        ## bandwidth 1
        my $values_combo_stats2 = $anchor->{ stats }{ all }[1];        ## bandwidth 2
        my $thresh_combo_stats1 = $anchor->{ stats }{ all }[2];        ## bandwidth 1
        my $thresh_combo_stats2 = $anchor->{ stats }{ all }[3];        ## bandwidth 2

        ## we are looping through all the periods of interest and selecting the bigger of 
        ## bandwidth 1 and bandwidth 2
        foreach my $index ( 0 .. $period_max_index ) {
        ## -----------------------------------------
            my $summary_combo = _create_summary_combo( $index,
                                   $values_combo_stats1,
                                   $values_combo_stats2,
                                   $thresh_combo_stats1,
                                   $thresh_combo_stats2,
                                  );
            $anchor->{ stats }{ summary }[$index] = $summary_combo;

        }
    }

}

=head3 get_period_info

IN:    Catalyst object
OUT:   Table headers 

=cut

sub get_period_info {
    my ( $c ) = @_;

     my $stats_periods =  [     
                { name              => 'day', 
                  decrements     => [ ( 1, 2, 3, 4, 5 ) ],
                #  titles         => [ 'Day-1', 'Day-2','Day-3','Day-3', 'Day-4', 'Day-5' ], 
                },
                { name        => 'week', 
                  decrements    => [ ( 1, 2, 3, 4 ) ],
                #  titles         => [ 'Week-1', 'Week-2','Week-3','Week-3', 'Week-4' ], 
                },
                { name        => 'month', 
                  decrements    => [ ( 1, 2, 3, 6, 12 ) ],
                #  titles        => [ 'Month-1', 'Month-2','Month-3','Month-6', 'Month-12' ], 
                } 
                 ];
     my $period_headers = [    ( 
                 'Day-1',   'Day-2',   'Day-3',   'Day-4',   'Day-5',    
                 'Week-1',  'Week-2',  'Week-3',  'Week-4', 
                 'Month-1', 'Month-2', 'Month-3', 'Month-6', 'Month-12', 
                )
                 ];

    return ( $stats_periods, $period_headers );
}

=head3 _get_period_search

creates an array of base DBIC search hrefs for each past period of interest

IN:    
    stats periods ... arrayref of hashrefs (from get_period_info sub): info for which days/weeks/months the stats
                                       should be displayed
    DateTime object for the current time

OUT:    DBIC search href:
                { 'day' }   => [ { year => ..., day   => ... }, ] 
                { 'week' }  => [ { year => ..., week  => ... }, ] 
                { 'month' } => [ { year => ..., month => ... }, ] 

=cut

sub _get_period_search ($$) {
    my (  $stats_periods, $dt  ) = @_;

    my ( $search );

    foreach my $period_href ( @$stats_periods ) {
        my $period        = $period_href->{ name }; 
        my $period_search = _get_period_search_href( $dt, $period, $period_href->{ decrements } );
            
        $search->{ $period }{ '-or' } = $period_search;
    }
    
    return $search;
}

=head3 _get_metric_search_info

creates the appropriate DBIC table and chooses the correct anchor field name for the given asset type and period

IN:    asset type:     3/4/5
    period:        day/week/month
OUT:    arrayref with table and anchor

=cut

sub _get_metric_search_info {
    my ( $asset_type, $period ) = @_;

    my ( $asset_abbr, $anchor ) = get_table_info( $asset_type );

    my $periodly = ( $period eq 'day' ) ? 'Daily' : ucfirst("${period}ly");
    my $table    = "MetricsStatus${periodly}${asset_abbr}";

    return ( $table, $anchor );
}

=head3 get_table_info

IN:     $asset_type
OUT:    array: $asset_abbr
        $anchor

=cut

sub get_table_info {
    my ( $asset_type ) = @_;

    $asset_type = ( $asset_type ) ? $asset_type : 3 ;

    my $asset_abbr = 'Sw';
    my $anchor     = 'datacentre';
    
    if ( $asset_type == 4 ) {
        $asset_abbr = 'Fw';
        $anchor     = 'midtower';
    }
    elsif ( $asset_type == 5 ) {
        $asset_abbr = 'Logtw';
        $anchor     = 'logtower';
    }
    
    return ( $asset_abbr, $anchor );
}

=head3 _get_period_search_href

create the base for a DBIC search href containing date

IN:     DateTime object
        day/week/month
        which period in the past (decrement to apply)

OUT:    search arrayref for use in a DBIC query

=cut

sub _get_period_search_href ($$$) {
    my ( $dt, $period, $decrements ) = @_;

    my ( $period_value, $period_search, $year, $week,
         $periodly, $table );

    my @decrements = @$decrements;

    my $search = [];

    my $period_dt = $dt->clone();
    my $index = 0;
    foreach my $decrem ( @decrements ) {

        my $by_how_much = ( $index == 0 ) ? $decrem : ( $decrem - $decrements[ $index - 1 ] );
        
        $period_dt = $period_dt->subtract( "${period}s" => $by_how_much );

        ( $year, $week ) = $period_dt->week;

        ## beware : when putting togethere 'order by' we use string cmp for ordering,
        ## depending on year being always after anything else 
        if ( $period eq 'week' ) {
            $period_search = {
                        year => $year,
                        week => $week,
                     };
        }
        elsif ( $period eq 'day' ) {
            $period_value = $period_dt->day_of_year;
            $period_search = {
                        week   => $week,
                        day    => $period_value,
                     };
        }            
        else {
            $period_value = $period_dt->month;
            $period_search = {
                        year   => $year,
                        month  => $period_value,
                     };
        }

        push @{$search}, $period_search;
        $index++;
    }

    return $search;
}

=head3 _get_anchors

get the basic information structure about:
    all datacentres/ particular datacentre midtowers/  particular midtower logtowers) 

IN:     Catalyst object
        N/A for asset type 3

        optional: 
        anchor id or arrayref of anchor ids (datacentre id/ids for asset type 3 
                            (midtower   id/ids for asset type 4)
        $is_parent ... if this is true, then anchor id/ids refer to midtowers rather than logical towers themselves
        (only applicable to asset_type 5)                 
OUT:    $anchors_href->{ $anchorname }{ anchors } = [{}, {}, ...] ... asset type 3
        $anchors_href->{ anchors }  = [{}, {}, ...] ................. asset type 4/5

=cut

sub _get_anchors ($$;$$) {
    my ( $c, $asset_type, 
         $anchor_value, $is_parent) = @_;

    my ( $dbic_anchors );
    if ( $asset_type == 3 ) {
        $dbic_anchors = get_datacentres( $c );
    }
    elsif ( $asset_type == 4 ) {
        $dbic_anchors = get_towers( $c, 'mid', 'name', 
                        'id',   $anchor_value);
    }
    else {
    ## Logical towers
        if ( $is_parent ) {
            $dbic_anchors = get_logical_towersByParent($c, 'name', $anchor_value);
        }
        else {
            $dbic_anchors = [ $c->model('DashboardDB::Asset')->search({ id => $anchor_value })->all ];
        }
    }

    my $anchors = _get_owners( $c, $asset_type, $dbic_anchors );    

    ## retrieve comments
    #_get_anchors_comments($c, $asset_type, $anchors );

    return $anchors;
}

sub _if_summary_NA {

    my ($index, 
        $values_combo_stats1, $values_combo_stats2) = @_;

    return 1 if ( not defined $values_combo_stats1->[$index]  or 
              not defined $values_combo_stats1->[$index][0] ) and 
            ( not defined $values_combo_stats2->[$index]      or
              not defined $values_combo_stats2->[$index][0] );

    return 1 if 
            defined $values_combo_stats1->[$index]            and 
            defined $values_combo_stats1->[$index][0]         and
                ( $values_combo_stats1->[$index][0] =~ /N\/A/ or
                  $values_combo_stats1->[$index][0] =~ /\A\s*\z/ ) 
                                and
            defined $values_combo_stats2->[$index]            and 
            defined $values_combo_stats2->[$index][0]         and
                ( $values_combo_stats2->[$index][0] =~ /N\/A/ or
                  $values_combo_stats2->[$index][0] =~ /\A\s*\z/ );

       return 0;

}

=head3 get_datacentres

=cut

sub get_datacentres {
    my ( $c ) = @_;

    my $datacentres  = [ $c->model('DashboardDB::Datacentre')
                   ->search({ '-and'  => [   'me.name' => { '!=' => 'BIN1'},
                            'me.name' => { '!=' => 'AAA'},
                            'me.name' => { '!=' => 'BBB'},
                            'me.name' => { '!=' => 'TEST'},
                            ],
                    },
                    {  join     =>     'territory',
                      '+select' => [   'territory.name' ],
                      '+as'     => [   'terrname'       ],
                      'order_by'=> [   'me.name'  ] } )->all ];
}

=head3 _get_owners

creates a hashref with territory id as key

IN:    type indicating which device stats we want:     3 ... for datacentres
                            4 ... for midtowers
                            5 ... for logtowers
    arrayref of datacentre DBIC objects
OUT:    hashref with info suitable for Metrics Status purposes:
            { $terrname } { anchorids }   = []
            { $terrname } { anchornames } = []
            { $terrname } { anchors }     = []

            { $dcid } { anchorids }       = []

            { $midtwid } { anchorids }    = []
=cut

sub _get_owners ($$$) {
    my ( $c, $asset_type, $dbic_objects ) = @_;

    return {} unless $asset_type == 3 or $asset_type == 4 or $asset_type == 5;

    my ( $anchors_href, $anchorid, $anchorname, $ownerid, $ownername, $ownermodel,
         $anchor_comment, $comment );

    my ( $anchor_href, $combo );

    my $csw_model   = _get_csw_models( $c, $dbic_objects )   if $asset_type == 3;
    my $afw_model   = _get_afw_models( $c, $dbic_objects )   if $asset_type == 4;
    my $logtw_model = _get_logtw_models( $c, $dbic_objects ) if $asset_type == 5;

    #$anchor_comment = _retrieve_comments(  $c, $asset_type, $dbic_objects );

    foreach my $dbic_object ( @$dbic_objects ) {
        if ( $asset_type == 3  ) {
            $anchorid   = $dbic_object->teFK;
            $anchorname = $dbic_object->get_column('terrname');
            $ownerid    = $dbic_object->dcID;
            $ownername  = $dbic_object->name;
            $ownermodel = ( $csw_model->{ $ownerid } ) ? $csw_model->{ $ownerid } : 0;
            next if ! $ownermodel;
            
            $combo = { assettypeid => $asset_type, anchorid => $ownerid };
            $comment = retrieve_comment( $c, $combo );
            #$comment = ( exists $anchor_comment->{ $ownerid } ) ?
            #            $anchor_comment->{ $ownerid }   : retrieve_comment( $c, $combo );
            $anchor_href = {
                        anchorname       => $ownername,
                        anchorid         => $ownerid,
                        anchormodel      => $ownermodel,
                        childassettypeid => ($asset_type + 1),
                        comment          => $comment,
                       };
        }
        elsif ( $asset_type == 4 ) {
            $anchorname = $dbic_object->get_column('dcid');
            $ownerid    = $dbic_object->id;
            $ownername  = $dbic_object->name;
            $ownermodel = ( $afw_model->{ $ownerid } ) ? $afw_model->{ $ownerid } : 0;
            next if ! $ownermodel;

            $combo = { assettypeid => $asset_type, anchorid => $ownerid };
            $comment = retrieve_comment( $c, $combo );
            #$comment = ( exists $anchor_comment->{ $ownerid } ) ?
            $anchor_href = {
                        anchorname       => $ownername,
                        anchorid         => $ownerid,
                        anchormodel      => $ownermodel,
                        childassettypeid => ($asset_type + 1), 
                        comment          => $comment,
                       };
        }
        else {
            $anchorname  = $dbic_object->parent;
            $ownerid     = $dbic_object->id;
            $ownername   = $dbic_object->name;
            $ownermodel  = ( $logtw_model->{ $ownerid } ) ? $logtw_model->{ $ownerid } : 0;
            next if ! $ownermodel;

            $combo = { assettypeid => $asset_type, anchorid => $ownerid };
            $comment = retrieve_comment( $c, $combo );
            $anchor_href = {
                        anchorname       => $ownername,
                        anchorid         => $ownerid,
                        anchormodel      => $ownermodel,
                        comment          => $comment,
                       };
        }

        push @{ $anchors_href->{ $anchorname }
                     ->{ anchornames }}, $ownername;
        push @{ $anchors_href->{ $anchorname }
                     ->{ anchorids }},   $ownerid;
        push @{ $anchors_href->{ $anchorname }
                     ->{ anchors }},     $anchor_href; 


    }

    return $anchors_href;
}

=head3 _get_comment_title

IN:    Catalyst object
    DBIC comment object or arrayref of DBIC comment objects
OUT:    either title object for the given comment object or arref of comment titles

=cut

sub _get_comment_title ($$) {
    my ( $c, $comments ) = @_;

    my $titles = [];

    ## collecting comment titles in bulk
    if ( ref $comments eq 'ARRAY') {
        foreach my $comment ( @$comments ) {
            my $title = $comment->title;
            push @$titles, [ $title->id, $title->title ];
        }
        return $titles;
    }
    ## collecting comment title for 1 anchor
    else {
        return [ $comments->title->id, $comments->title->title ];
    }

}

=head3 _get_comment_details

IN:     Catalyst object
        DBIC comment object or arrayref of DBIC comment objects
OUT:

=cut

sub _get_comment_details ($$) {
    my ( $c, $comments ) = @_;

    ## collecting comment details in bulk
    if ( ref $comments eq 'ARRAY') {

        my $details = [];
        my $throughput_sums = [];
        foreach my $comment ( @$comments ) {
            my $this_comment_details;
            my $throughput_sum = 0;
            if ( $comment ) {
                my @comment_details = $comment->details
                                  ->search({}, { 'order_by'   => 
                                        { '-desc' => 
                                             [ 'throughput' ] }});
                foreach my $detail ( @comment_details ) {
                    my $customer   = ( $detail->customer )   ? $detail->customer   : '';
                    my $throughput = ( $detail->throughput ) ? $detail->throughput : '';
                    push @$this_comment_details, [ $detail->id, $customer, $throughput ];

                    $throughput_sum += $detail->throughput;
                }
            }
            push @$details,         $this_comment_details;
            push @$throughput_sums, $throughput_sum;
        }

        return ( $details, $throughput_sums );
    }
    ## collecting comment details for 1 anchor
    else {
        my $this_comment_details = [];
        my @comment_details = $comments->details
                           ->search({}, { 'order_by'    => 
                                            { '-desc' => [ 'throughput' ] }});
        my $throughput_sum = 0;
        foreach my $detail ( @comment_details ) {
            my $customer   = $detail->customer;
            my $throughput = $detail->throughput;
            push @$this_comment_details, [ $detail->id, $customer, $throughput ];
            $throughput_sum += $detail->throughput;
        }

        return ( $this_comment_details, $throughput_sum );
    }

}

=head3 _get_anchors_comments

appends comment info to the anchor href
(used during page load)

IN: $c
    $asset_type
    $anchors

=cut

sub _get_anchors_comments {
    my ( $c, $asset_type, $anchors ) = @_;

    foreach my $anchorname ( keys %$anchors ) {
        my $anchor_ids = $anchors->{ $anchorname }{ anchorids };
        
        ## comments under an anchor for all the owners
        #  (datacentre => firewalls)
        my @comment_dbics = $c->model('DashboardDB::MetricsStatusComment')->search(
                            { 
                                anchor     => { '-in' => $anchor_ids },
                                asset_type => $asset_type,
                            },
                            {
                                select     => [ 'id' ],
                            } )->all;

        if ( not scalar @comment_dbics ) {
            push @{ $anchors->{ $anchorname }
                    ->{ comment_titles }},  []; 
            push @{ $anchors->{ $anchorname }
                         ->{ comment_details }}, []; 

            next;

        }

        my @all_comments = ();
        foreach my $anchor_id ( @$anchor_ids ) {
            my $comment;
            foreach my $this_comment( @comment_dbics ) {
                $comment = $this_comment && last if $anchor_id == $this_comment->anchor;
            }
            push @all_comments, $comment;
        }

        my $titles  = _get_comment_title( $c, \@all_comments );
        push @{ $anchors->{ $anchorname }
                        ->{ comment_titles }},  $titles; 

        my ( $details, $throughput_sums ) = _get_comment_details( $c, \@all_comments );
        push @{ $anchors->{ $anchorname }
                        ->{ comment_details }},     $details; 
        push @{ $anchors->{ $anchorname }
                        ->{ comment_throughputs }}, $throughput_sums; 
    
    }

}

=head3 _get_csw_models

gets models of core switches associated with datacentres

IN:     $c, 
        $dbic_objects (Datacentres))

OUT:    hashref associating Datacentre IDs and the corresponding CORE SWITCH models

=cut

sub _get_csw_models {

    my ( $c, $dbic_objects ) = @_;

    ## get csw asset for a datacentre
    my @dc_ids = map { $_->dcID } @$dbic_objects;

    my $csw_objects = [ $c->model('DashboardDB::Asset')->search(
                            {
                              name   => { like => '%csw-%-core%' },
                             'dc.dcID' => { '-in' => \@dc_ids } 
                            },
                            {
                             'join'    => { 'bay' => 
                                            { 'aisle' => 
                                                { 'room' => 
                                                    { 'floor' => 'dc' }}}},
                              select   => [  qw(dc.dcID model ) ],
                              as       => [  qw( id     model) ],
                             'order_by'  => { '-asc' => [ 'name' ] }
                            }
                            )->all ];

    ## create a href
    my $csw_model = {};
    foreach my $csw ( @$csw_objects ) {
        my $id    = $csw->get_column('id');
        next if exists $csw_model->{ $id };

        my $model = $csw->get_column('model');
        $csw_model->{ $id } = $model;
    }

    return $csw_model;

} 

=head3 _get_afw_models

gets models of firewalls associated with midtowers 

IN:    $dbic_object 

OUT:    model of firewall

=cut

sub _get_afw_models {

    my ( $c, $dbic_objects ) = @_;

    my @midtw_ids = map { $_->id } @$dbic_objects;

    my $afw_objects = [ $c->model('DashboardDB::Asset')->search(
                            {  clFK    => 4, 
                               name    => {  like => '%afw-001%' },
                              'parent' => { '-in' => \@midtw_ids } },
                            {
                               select  => [  qw( parent model ) ],
                               as      => [  qw( parent model) ] }
                                   )->all ];

    ## create a href
    my $afw_model = {};
    foreach my $afw ( @$afw_objects ) {
        my $parent = $afw->get_column('parent');
        my $model  = $afw->get_column('model');
        $afw_model->{ $parent } = $model;
    }

    return $afw_model;
}

=head3 _get_logtw_models

gets models of core switches associated with datacentres

IN:    $dbic_objects 
OUT:   model of logtower servers (all the same within a logtower)

=cut

sub _get_logtw_models {

    my ( $c, $dbic_objects ) = @_;

    my @logtw_ids = map { $_->id } @$dbic_objects;

    my $sss_objects = [ $c->model('DashboardDB::Asset')->search(
                                            {  clFK     => 5, 
                                               name     => {  like => '%sss%' },
                                              'parent'  => { '-in' => \@logtw_ids } },
                                            {
                                               select   => [  qw( parent model ) ],
                                               as       => [  qw( parent     model) ],
                                            }
                                                 )->all ];

    ## create a href
    my $sss_model = {};
    foreach my $afw ( @$sss_objects ) {
        my $parent = $afw->get_column('parent');
        my $model  = $afw->get_column('model');
        $sss_model->{ $parent } = $model;
    }

    return $sss_model;

}

=head2 _populate_values 

gets peak and 95th percentile stats values for the required datacentre/midtower/logical tower for all displayed periods

IN:     Catalyst object
        asset type
        period 

=cut

sub _populate_values {
    my ( $c, $asset_type, $anchor_id ) = @_;

    

}

=head3 _create_summary_combo


=cut

sub _create_summary_combo {
    my ( $index,
         $values_combo_stats1,
         $values_combo_stats2,
       ) = @_;


    my ($vpeak1, $vpeak2, $v95th1, $v95th2);
    my ($tpeak1, $tpeak2, $t95th1, $t95th2);
    my $summary_combo = [ 'N/A','N/A',0 , 0 ];

    if (defined $values_combo_stats1 and defined $values_combo_stats2) {

        do {
            return $summary_combo;
        } if _if_summary_NA($index, 
                    $values_combo_stats1, $values_combo_stats2);
        

        if ( ( defined $values_combo_stats1->[$index][0]  && 
                   $values_combo_stats1->[$index][0] =~ /\A[0-9,.]+\z/ )
                    &&
             ( defined $values_combo_stats2->[$index][0]  &&
                   $values_combo_stats2->[$index][0] =~ /\A[0-9,.]+\z/ )) { 

            $vpeak1 = $values_combo_stats1->[$index][0];
            $vpeak2 = $values_combo_stats2->[$index][0];
            $v95th1 = $values_combo_stats1->[$index][1];
            $v95th2 = $values_combo_stats2->[$index][1];
            $tpeak1 = $values_combo_stats1->[$index][2];
            $tpeak2 = $values_combo_stats2->[$index][2];
            $t95th1 = $values_combo_stats1->[$index][3];
            $t95th2 = $values_combo_stats2->[$index][3];
            $summary_combo->[0] = ( $vpeak1 >= $vpeak2 ) ? $vpeak1 : $vpeak2;
            $summary_combo->[1] = ( $v95th1 >= $v95th2 ) ? $v95th1 : $v95th2;
            $summary_combo->[2] = ( $vpeak1 >= $vpeak2 ) ? $tpeak1 : $tpeak2;
            $summary_combo->[3] = ( $v95th1 >= $v95th2 ) ? $t95th1 : $t95th2;
        }
        elsif ( defined $values_combo_stats1->[$index][0] && 
                $values_combo_stats1->[$index][0] =~ /[0-9,.]+/ ) {
            $vpeak1 = $values_combo_stats1->[$index][0];
            $v95th1 = $values_combo_stats1->[$index][1];
            $tpeak1 = $values_combo_stats1->[$index][2];
            $t95th1 = $values_combo_stats1->[$index][3];
            $summary_combo->[0] = $vpeak1;
            $summary_combo->[1] = $v95th1;
            $summary_combo->[2] = $tpeak1;
            $summary_combo->[3] = $t95th1;
        }
        elsif ( defined $values_combo_stats2->[$index][0] ) {
            $vpeak2 = $values_combo_stats2->[$index][0];
            $v95th2 = $values_combo_stats2->[$index][1];
            $tpeak2 = $values_combo_stats2->[$index][2];
            $t95th2 = $values_combo_stats2->[$index][3];
            $summary_combo->[0] = $vpeak2;
            $summary_combo->[1] = $v95th2;
            $summary_combo->[2] = $tpeak2;
            $summary_combo->[3] = $t95th2;
        }
    }
    elsif ( defined $values_combo_stats1 && defined $values_combo_stats1->[$index][0] ) {
        $vpeak1 = $values_combo_stats1->[$index][0];
        $v95th1 = $values_combo_stats1->[$index][1];
        $tpeak1 = $values_combo_stats1->[$index][2];
        $t95th1 = $values_combo_stats1->[$index][3];
        $summary_combo->[0] = $vpeak1;
        $summary_combo->[1] = $v95th1;
        $summary_combo->[2] = $tpeak1;
        $summary_combo->[3] = $t95th1;
    }    
    elsif ( defined $values_combo_stats2 && defined $values_combo_stats2->[$index][0]) {
        $vpeak2 = $values_combo_stats2->[$index][0];
        $v95th2 = $values_combo_stats2->[$index][1];
        $tpeak2 = $values_combo_stats2->[$index][2];
        $t95th2 = $values_combo_stats2->[$index][3];
        $summary_combo->[0] = $vpeak2;
        $summary_combo->[1] = $v95th2;
        $summary_combo->[2] = $tpeak2;
        $summary_combo->[3] = $t95th2;
    }    

    return $summary_combo;

}


=head3 get_datacentre_info

gets mid towers and firewalls for a given datacentre
creates a data structure suitable for presentation on the Metrics Status page

IN:     Catalyst context object
        id/name ... which datacentre property to search by
        value   ... its value (12345/T060))
OUT:    array of 2 arrayrefs:  mid-tower info
                   firewall info

=cut

sub get_datacentre_info {
    my ($c, 
    $field, $value) = @_;

    ## mid-towers
    my @towers = get_towers( $c, 'mid', $field, $value );        
    my @tower_info    = map { { id => $_->id, name => $_->name } } 
                        sort { $a->id <=> $b->id } @towers;

    ## firewalls (show traffic to the whole mid-tower)
    my @firewalls = get_firewalls( $c, $field, $value );        
    my @firewall_info = map { { id => $_->id, name => $_->name, midtower => $_->parent } } 
                        sort { $a->id <=> $b->id } @firewalls;

    return (\@tower_info, \@firewall_info);

}

=head3 get_base_info

sets up info common for multiple actions

IN:    Catalyst object
OUT:    $period_data, $anchortype_info, $assets_stats_info

=cut

sub get_base_info {
    my ( $c ) = @_;

    my $period_data;
    ( $period_data->{ period_info }, $period_data->{ period_headers } ) = get_period_info( $c ); 
    my $anchortype_info = {
                    '3' => { 
                        anchortype  => 'dc',
                        assettype   => 'csw',
                        assetlabel  => 'CORE SWITCH',
                        stats_type  => 'b',
                    },
                    '4' => { 
                        anchortype  => 'midtw',
                        assettype   => 'afw',
                        assetlabel  => 'FIREWALL',
                        stats_type  => 'b',
                    },
                    '5' => { 
                        anchortype  => 'logtw',
                        assettype   => 'sss',
                        assetlabel  => 'LOGICAL TOWERS',
                        stats_type  => 's',

                    },
                 };

    my $assets_stats_info = get_stats_info( $c );

    return ( $period_data, $anchortype_info, $assets_stats_info );
}

=head3 _get_thresholds

provides info for creating Traffic Lights
retrieves information about thresholds set for individual anchors

IN:     $c, $asset_type, 
        stats_data .....  stats_type ... d (detail) / s (summary) / b (both)
        $anchors
OUT:    href of threshold info (arrayref of thresholds for each stats):
                { $asset_type }{ $anchor_id } = [
                                    [ peak_thresh, 95th_thresh ],
                                    [ peak_thresh, 95th_thresh ],
                                    [ peak_thresh, 95th_thresh ],
                                    [ peak_thresh, 95th_thresh ],
                                        ...
                                        ...
                                ]
=cut

sub _get_thresholds {
    my ( $c, $asset_type, $anchors ) = @_;

    my @anchor_ids = map { @{$anchors->{ $_ }{ anchorids }} } keys %$anchors;

    my $threshold = {};

    ## get dbic info for asset_type and supplied anchors
    my ( $asset_abbr, $anchor ) = get_table_info( $asset_type );
    my $table = "MetricsStatusThreshold${asset_abbr}";

    my @thresh_dbics = $c->model("DashboardDB::$table")
              ->search( { $anchor => { '-in' => \@anchor_ids } } )->all;    
    return $threshold unless scalar @thresh_dbics;

    ## arrange into a hashref structure
    foreach my $thresh_obj ( @thresh_dbics ) {
        $threshold->{ $thresh_obj->$anchor }
                { $thresh_obj->metric }
                { $thresh_obj->period_type }[0] = $thresh_obj->orange_peak;
        $threshold->{ $thresh_obj->$anchor }
                { $thresh_obj->metric }
                { $thresh_obj->period_type }[1] = $thresh_obj->orange_95th;
        $threshold->{ $thresh_obj->$anchor }
                { $thresh_obj->metric }
                { $thresh_obj->period_type }[2] = $thresh_obj->red_peak;
        $threshold->{ $thresh_obj->$anchor }
                { $thresh_obj->metric }
                { $thresh_obj->period_type }[3] = $thresh_obj->red_95th;

    }

    return $threshold;
}

=head3 retrieve_thresholds

IN:     Catalyst object
        assetinfo href:    
            assettypeid
            anchorid
            metricid
            modelid
            periodname
OUT:    href of threshold info for a specific cell:
                            current => { peak   => 11,
                                        '95th'  => 22 },
                            default => { peak   => 33,
                                        '95th'  => 44 },

=cut

sub retrieve_thresholds {

    my ( $c, $assetinfo ) = @_;

    my ( $asset_abbr, $anchor )         = get_table_info( $assetinfo->{ assettypeid } );
    my ( $current_thresh, $default_thresh ) = _get_raw_thresholds( $c, $assetinfo );

    my $model_obj = $c->model('DashboardDB::Model')->find({ id => $assetinfo->{ modelid }} );

    ## massage into suitable href
    my $current_orange_peak  = ( $current_thresh )                 ? 
                     $current_thresh->get_column('orange_peak') : 'Not set';
    my $current_orange_95th  = ( $current_thresh )                 ? 
                     $current_thresh->get_column('orange_95th') : 'Not set';
    my $current_red_peak      = ( $current_thresh )                 ? 
                     $current_thresh->get_column('red_peak') : 'Not set';
    my $current_red_95th       = ( $current_thresh )                 ? 
                     $current_thresh->get_column('red_95th') : 'Not set';

    my $set2default        = ( $current_thresh )                 ? 
                     $current_thresh->get_column('set2default') : 0;

    my $default_orange_peak  = ( $default_thresh )                 ? 
                     $default_thresh->get_column('orange_peak') : 'Not set';
    my $default_orange_95th  = ( $default_thresh )                 ? 
                     $default_thresh->get_column('orange_95th') : 'Not set';
    my $default_red_peak      = ( $default_thresh )                 ? 
                     $default_thresh->get_column('red_peak') : 'Not set';
    my $default_red_95th       = ( $default_thresh )                 ? 
                     $default_thresh->get_column('red_95th') : 'Not set';

    
    my $threshold = {
                current =>  { 
                       orange => {
                             peak   => $current_orange_peak,
                                 '95th'  => $current_orange_95th,
                             },
                       red =>    {
                             peak   => $current_red_peak,
                                 '95th'  => $current_red_95th,
                             },
                    },
                default  => { 
                       orange => {
                             peak   => $default_orange_peak,
                                 '95th'  => $default_orange_95th 
                             },
                       red =>    {
                             peak   => $default_red_peak,
                                 '95th'  => $default_red_95th 
                             },
                    },
                set2default => $set2default,
                modelname   => $model_obj->name,
                };

    return $threshold;
}

=head3 update_thresholds

IN:     Catalyst object
        assetinfo href:    
            assettypeid
            anchorid
            metricid
            modelid
            modeltype
            periodname
        threshold data ... href for current and default thresholds
OUT:    1 ... updated
        2 ... failed

=cut

sub update_thresholds ($$$) {

    my ( $c, $assetinfo, $threshold_data ) = @_;

    return 2 unless $assetinfo->{ anchormodel };

    ## threshold values
    ##-----------------
    my $current_orange_peak = ( _is_thresh_input_valid($threshold_data, 'current_orange_peak') ) ? 
                    $threshold_data->{current_orange_peak } : undef;
    my $current_orange_95th = ( _is_thresh_input_valid($threshold_data, 'current_orange_95th') ) ? 
                    $threshold_data->{current_orange_95th } : undef;
    my $current_red_peak     = ( _is_thresh_input_valid($threshold_data, 'current_red_peak') )    ?
                    $threshold_data->{current_red_peak } : undef;
    my $current_red_95th     = ( _is_thresh_input_valid($threshold_data, 'current_red_95th') )    ?
                    $threshold_data->{current_red_95th } : undef;

    my $default_orange_peak = ( _is_thresh_input_valid($threshold_data, 'default_orange_peak') ) ?
                    $threshold_data->{default_orange_peak } : undef;
    my $default_orange_95th = ( _is_thresh_input_valid($threshold_data, 'default_orange_95th') ) ?
                    $threshold_data->{default_orange_95th } : undef;
    my $default_red_peak     = ( _is_thresh_input_valid($threshold_data, 'default_red_peak') )    ?
                    $threshold_data->{default_red_peak } : undef;
    my $default_red_95th     = ( _is_thresh_input_valid($threshold_data, 'default_red_95th') )    ?
                    $threshold_data->{default_red_95th } : undef;

    my ( $asset_abbr, $anchor ) = get_table_info( $assetinfo->{ assettypeid } );

    ## get current threshold
    ##----------------------
    my ( $table, $search, $metric_search, $update );

    ## update the asset thresholds
    ##----------------------------------
    ## bandwidth in and out have the same thresholds
    if ( $assetinfo->{ metricid } == 1 || $assetinfo->{ metricid } == 2 ) {
        $metric_search = { '-in' => [ (1, 2) ] };
    }
    else {
        $metric_search = $assetinfo->{ metricid };
    }

    $search = {
            $anchor     => $assetinfo->{ anchorid },
            metric      => $metric_search,
            period_type => $assetinfo->{ periodname },
          };

    $update = {
            model       => $assetinfo->{ anchormodel },
            set2default => $assetinfo->{ set2default } || '0',
            orange_peak => $current_orange_peak,
            orange_95th => $current_orange_95th,
            red_peak    => $current_red_peak,
            red_95th    => $current_red_95th,
            user         => $c->session->{ login },
          };

    $table = "DashboardDB::MetricsStatusThreshold$asset_abbr";
    my $success1 = do_thresh_update( $c, $table, $search, $update );

    ## update the threshold defaults
    ##-------------------------------------
    $search = {
            asset_type  => $assetinfo->{ assettypeid },
            model       => $assetinfo->{ anchormodel },
            metric      => $metric_search,
            period_type => $assetinfo->{ periodname },
          };

    $update = {
            orange_peak => $default_orange_peak,
            orange_95th => $default_orange_95th,
            red_peak    => $default_red_peak,
            red_95th    => $default_red_95th,
            user         => $c->session->{ login },
          };

    $table = 'DashboardDB::MetricsStatusThresholdDefaults';
    my $success2 = do_thresh_update( $c, $table, $search, $update );

    my $end_success = ( $success1 > $success2 ) ? $success1 : $success2;
    $c->cache->set('capstatsdata', undef) if $end_success == 1;

    return $end_success ;
}

=head3 retrieve_comment

retrieves comments for one anchor

IN:     Catalyst object
        assetinfo href:    
            assettypeid
            anchorid

=cut

sub retrieve_comment {

    my ( $c, $assetinfo ) = @_;

    ## get comment
    my $comment = _get_anchor_comment( $c, 
                       $assetinfo->{ assettypeid }, $assetinfo->{ anchorid }, );

    return $comment if not $comment;

    ## get title
    my ( $title_text, $title_id );
    my $title      = _get_comment_title( $c,  $comment );

    ## get details
    my ( $details, $throughput ) = _get_comment_details( $c,  $comment );

    return {    title           => $title,
                details         => $details,
                throughput_sum  => $throughput,
                id              => $comment->id,
           };
}

=head3 _retrieve_comments

retrieves information about comments set for individual anchors

IN:     $c, $asset_type, 
        stats_data .....  stats_type ... d (detail) / s (summary) / b (both)
        $anchors
OUT:    href of comment info (arrayref of comments for each stats):
                    { $anchor_id } = [
                                    [ customer, throughput ],
                                    [ customer, throughput ],
                                    [ customer, throughput ],
                                        ...
                                        ...
                                ]
=cut

sub _retrieve_comments {
    my ( $c, $asset_type, $anchors ) = @_;

    my $comments = {};

    my $id_column = ( $asset_type == 3 ) ? 'dcID' : 'id';
    my @anchor_ids = map { $_->id } @$anchors;

    my $comment = {};
    my $search  = { 
            anchor      => { '-in' => \@anchor_ids},
            asset_type  => $asset_type,
              };

    my @comment_dbics = $c->model("DashboardDB::MetricsStatusComment")
                  ->search( $search,
                                    { 
                       join   => [ 'title' , 'details' ],
                       select => [ qw/me.id me.anchor title.id  title.title 
                                      details.id  details.customer details.throughput
                             /], 
                       as      => [ qw/id    anchor    titleid  titletext       
                                       detailid   customer         throughput      
                                 / ],
                       order_by => { '-desc' => 'throughput' },
                    },
                   )->all();

    my $throughput_sum = 0;

    foreach my $comment_row ( @comment_dbics ) {

        my $this_anchorid = $comment_row->get_column('anchor');
        $comment->{ $this_anchorid }{ id } = $comment_row->get_column('id'); 
        $comment->{ $this_anchorid }{ title } = 
                                [ $comment_row->get_column('titleid'),
                              $comment_row->get_column('titletext') ];

        my $detailid = $comment_row->get_column('detailid');

        if ( $detailid ) {
            push @{$comment->{ $this_anchorid }{ details }}, 
                [ $detailid,
                  $comment_row->get_column('customer'),
                  $comment_row->get_column('throughput') ];
            $comment->{ $this_anchorid }{ throughput_sum } += 
                        $comment_row->get_column('throughput_sum'); 
        }

    }

    return $comment;
}

=head3 _is_thresh_input_valid

tests for valid input of threshold data  

=cut

sub _is_thresh_input_valid ($$) {
    my ($threshold_data, $key) = @_;

    my $is_valid = ( $key                   &&
             exists $threshold_data->{ $key } && 
             $threshold_data->{ $key } =~ /\A[0-9,.\-]+\z/ ); 

    return $is_valid;
}

=head3 do_thresh_update

=cut

sub do_thresh_update ($$$$) {
    my ( $c, $table, $search, $update ) = @_;

    my $success = 2;

    my ( @rows );

    eval {
        @rows = $c->model( $table )->search( $search );
    };
    
    do {
        if ( scalar @rows ) {
            foreach my $row ( @rows ) {
                eval {
                    $row->update( $update );
                };
                return 2 if $@;
            }
        }
        else {
            my %create = ( %$search, %$update );
            if ( ref $create{ metric } ) {
                eval {
                    $create{ metric } = 1;
                    $c->model( $table )->create( \%create );
                    $create{ metric } = 2;
                    $c->model( $table )->create( \%create );
                };
                return 2 if $@;
            }
            else {
                eval {
                    $c->model( $table )->create( \%create );
                };
                return 2 if $@;
            }
        }

        $success = 1;

    } unless $@;
    
    return $success;
}

=head3 destroy_comment_detail

IN:     $c
        $search ... {   anchor     => ...,
                        asset_type => ... }
        $update ... {   title      => ...,
                        details    => [ { id => ..., customer => ..., throughput => ... },
                                        { id => ..., customer => ..., throughput => ... },
                                    ...] }
=cut

sub destroy_comment_detail {
    my ( $c, $id ) = @_;

    eval {
        $c->model('DashboardDB::MetricsStatusCommentDetail')
          ->find($id)
          ->delete();
    };

    return 2 if $@; 

    return 1;
}


=head3 update_comment

IN:     $c
        $search ... { anchor     => ...,
                      asset_type => ... }
        $update ... { title      => ...,
                      details    => [ { id => ..., customer => ..., throughput => ... },
                                      { id => ..., customer => ..., throughput => ... },
                     ...] }

OUT:    for title:    $success
        for detail:    ($success, $new_commentid)
=cut

sub update_comment {
    my ( $c, $which, $search, $update ) = @_;

    my $success = 1;

    if ( $which eq 't' ) {
        eval {
            $c->model('DashboardDB::MetricsStatusCommentTitle')->find($search)->update($update);
        };
        $success = 2 if $@;

        return $success;
    }
    else {
        my $id = $search->{ id };
        my ( $new_comment, $new_commentid );
        my %update_or_create = ( %$search, %$update );

        eval {
            my $comment = ($c->model('DashboardDB::MetricsStatusCommentDetail')->search($search)->all())[0];

            if ( $comment ) {
                $comment->update($update);
            }
            else {

                $new_comment = $c->model('DashboardDB::MetricsStatusCommentDetail')->create( $update );
                #$new_commentid = $new_comment->id if $id =~ /\AN/;
                $new_commentid = $new_comment->id;
            }
        };
        $success = 2 if $@;

        
        $c->cache->set('capstatsdata', undef) if $success == 1;

        return ( $success, $new_commentid );
    }
}


=head3 _get_raw_thresholds

IN:     Catalyst object
        assetinfo href:    
            assettypeid
            anchorid
            metricid
            modelid
            periodname
OUT:    href of threshold info for a specific cell:
                                current => { peak   => 11,
                                '95th'  => 22 },
                                default => { peak   => 33,
                                '95th'  => 44 },

=cut

sub _get_raw_thresholds ($$) {

    my ( $c, $assetinfo ) = @_;

    my ( $asset_abbr, $anchor ) = get_table_info( $assetinfo->{ assettypeid } );

    ## get current threshold
    my ($search);
    $search = { 
            $anchor     => $assetinfo->{ anchorid },
            metric      => $assetinfo->{ metricid },
            period_type => $assetinfo->{ periodname },
          };

    my $current_thresh = $c->model("DashboardDB::MetricsStatusThreshold$asset_abbr")
                           ->find( $search,
                                    { 
                                        select   => [ qw(orange_peak orange_95th red_peak red_95th set2default) ],
                                        as       => [ qw(orange_peak orange_95th red_peak red_95th set2default) ],
                                    } 
                                  );
    ## get default threshold
    $search = { 
            asset_type  => $assetinfo->{ assettypeid },
            model       => $assetinfo->{ modelid },
            metric      => $assetinfo->{ metricid },
            period_type => $assetinfo->{ periodname },
          };

    my $default_thresh = $c->model("DashboardDB::MetricsStatusThresholdDefaults")
                           ->find( $search,
                                    { 
                                        select   => [ qw(orange_peak orange_95th red_peak red_95th) ],
                                        as       => [ qw(orange_peak orange_95th red_peak red_95th) ],
                                    } 
                                  );

    return ( $current_thresh, $default_thresh );

}

=head3 _get_anchor_comment

returns comment title and comment details for the specified anchor (datacentre, midtower, logical tower)

=cut

sub _get_anchor_comment ($$$) {
    my ( $c, $asset_type, $anchor_id ) = @_;

    my ( $search, $comment );
    $search = { 
            anchor      =>  $anchor_id,
            asset_type  =>  $asset_type,
          };

    $search->{ user } = $c->session->{ login };
    $comment = $c->model('DashboardDB::MetricsStatusComment')->find_or_create( $search );

    $comment->find_or_create_related('title', { title => '' });

    return $comment;
}

=head3 set_thresh2default

IN:     $c
        $assetinfo
        $default_thresh

OUT:    ( $success, arref of affected asset ids )

=cut

sub set_thresh2default {
    my ( $c, $db_info, $assetinfo, $default_thresh ) = @_;

    my $success = 2;

    my ( $asset_abbr, $anchor ) = ( $db_info->{ asset_abbr },  $db_info->{ anchor });
    my $table = "MetricsStatusThreshold${asset_abbr}";

    my $orange_peak = $default_thresh->{ default_orange_peak }; 
    my $orange_95th = $default_thresh->{ default_orange_95th };
    my $red_peak     = $default_thresh->{ default_red_peak };
    my $red_95th     = $default_thresh->{ default_red_95th };

    my $search = { 
                    model       => $assetinfo->{ anchormodel },
                    metric      => $assetinfo->{ metricid },
                    period_type => $assetinfo->{ periodname },
                    set2default => '1',
                };
    my $update = {
                    orange_peak => $orange_peak,
                    orange_95th => $orange_95th,
                    red_peak    => $red_peak,
                    red_95th    => $red_95th,
                    user         => $c->session->{ login },
                };

    my @anchor_ids = ();
    eval {
        my @rows = $c->model("DashboardDB::$table")->search( $search );
        map { $_->update( $update ); push @anchor_ids, $_->$anchor; } @rows;
    };

    $success = 1 unless $@;

    return ( $success, \@anchor_ids );

}

sub is_valid_thresh_input {
    my $input = @_;
    my $matches = ( $input =~ /\A[0-9,.\-]+\z/ ) || 0;

    return $matches;
}

sub cleanup_spaces {
    my $input = @_;
    $input =~ s/\A\s+//;
    $input =~ s/\s+\z//;

    return $input;
}

=head3 change_model4capacity

determines if we need to change the capacity_status threshold related tables for the corresponding anchor(s)

IN:     $c
        $asset    ... DBIC object 
        $is_bulk  ... shows whether the model change applies to a single asset or multiple ones
                      this is relevant for servers, because all servers in a tower should have the same model

=cut

sub change_model4capacity {
    my ($c, $asset, $is_bulk) = @_;
    
    my $asset_name  = $c->request->parameters->{name};
    return if    ($asset_name =~ /\Asss\-/ && not $is_bulk) or $asset_name =~ /:/;
    return unless $asset_name =~ /\Acsw\-/ || $asset_name =~ /\Aafw\-/ ;

    my $new_modelid = $c->request->parameters->{model};

    if  ( 
         ( $asset->model && $asset->model->id  &&
           $new_modelid  && ($new_modelid > 0) &&
          ($asset->model->id != $new_modelid )) ) {

        return 1;
    }
    return;
}

=head3 update_capacity_thresholds_model

=cut

sub update_capacity_thresholds_model {
    my ( $c, $asset ) = @_;

    my $asset_type = $asset->clFK;
    return unless $asset_type == 3 || $asset_type == 4 || $asset_type == 5;

    my ( $asset_abbr, $anchor_column ) = get_table_info( $asset_type );
    my $table = "MetricsStatusThreshold$asset_abbr";

    _update_model4anchor($c, $table, $anchor_column, $asset );

}

=head3 _get_anchor4asset

    returns Metrics Status anchor for the given asset

    for switch   (asset type = 77) ... datacentre id
    for firewall (asset type = 88 ... midtower id
    for server   (asset type = 99) ... logtower id

IN:     Catalyst object
        asset DBIC object
OUT:    anchor id

=cut

sub _get_anchor4asset {
    my ( $asset ) = @_;

    my ($asset_type, $bay, $aisle, $floor, $room, $datacentre);

    $asset_type = $asset->clFK;
    if ( $asset_type == 3 ) {
        $bay = $asset->bay;
        if ( $bay ) {
            $aisle = $bay->aisle;
            if ( $aisle ) {
                $room = $aisle->room;
                if ( $room ) { 
                    $floor  = $room->floor;
                    if ( $floor ) {
                        $datacentre = $floor->dc;
                        if ( $datacentre ) {
                            return $datacentre->dcID;
                        }
                    }
                }
            }
        }
    }
    elsif ( $asset_type == 4 || $asset_type == 5 ) {
        return $asset->parent->id if     $asset->parent->id && $asset->parent->id > 0;
    }
     
    return;
}

=head3 _update_model4anchor

does the update of the model in the MetricsStatusThresholdSw etc depending on the asset type and anchor id

=cut

sub _update_model4anchor {

    my ($c, $table, $anchor_column, $asset ) = @_;

    ## get the anchor for this asset
    my $anchor_id = _get_anchor4asset($asset);

    return unless $anchor_id;

    my $new_modelid = $c->request->parameters->{model};

    ## is there a default threshold for this asset type and new model:
    ##    if there is, check the threshold values and if they are the same 
    ##    as this anchor's ones, set the set2default to 1
    my $set2default = _set_default_threshold_flag($c, $table,     $asset->clFK, 
                                                  $anchor_column, $anchor_id,
                                                  $new_modelid);

    foreach my $period_type ( keys %$set2default ) {
        foreach my $metric ( keys %{$set2default->{ $period_type }} ) {
            my $this_set2default = $set2default->{ $period_type }{ $metric } || '0';
            $c->model("DashboardDB::$table")->search({ $anchor_column => $anchor_id,
                                                  metric         => $metric,
                                                  period_type    => $period_type,
                                                })
                                       ->update({ model          => $new_modelid,
                                                  set2default    => $this_set2default });
        }
    }

}

=head3 _set_default_threshold_flag
decide whether the asset anchor needs to have the set2default flag set

OUT:    href of set2default for day/week/month

=cut

sub _set_default_threshold_flag {
    my ($c,  $table, $asset_type,  
        $anchor_column, $anchor_id, 
        $new_modelid) = @_;

    my @individual_ones = $c->model("DashboardDB::$table")->search({ $anchor_column => $anchor_id },);
    my $set2default =     
            {
                day   => {},
                week  => {},
                month => {},
            };

    ## not we check each threshold row
    foreach my $individual ( @individual_ones ) {
        my $period_type = $individual->period_type;
        my $metric     = $individual->metric;
        my $default = $c->model("DashboardDB::MetricsStatusThresholdDefaults")
                        ->find({    asset_type  => $asset_type,
                                    model       => $new_modelid,
                                    period_type => $period_type,
                                    metric      => $metric,
                               });
        next unless $default;

        my $orange_peak_def = $default->orange_peak;
        my $orange_95th_def = $default->orange_95th;
        my $red_peak_def    = $default->red_peak;
        my $red_95th_def    = $default->red_95th;

        if ( $individual->orange_peak =~ /\A$orange_peak_def\z/ &&
             $individual->orange_95th =~ /\A$orange_95th_def\z/ &&
             $individual->red_peak =~ /\A$red_peak_def\z/       &&
             $individual->red_95th =~ /\A$red_95th_def\z/
           ) {
            $set2default->{ $period_type }{ $metric } = '1';
        }
                 
    }
    return $set2default;

}

1;
