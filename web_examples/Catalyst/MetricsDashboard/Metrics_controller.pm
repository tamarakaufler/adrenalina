package Dashboard::Controller::Metrics;

use v5.10;

use Moose;
use namespace::autoclean;

use FindBin                    qw($Bin);
use lib                        "$FindBin::Bin/../..";

use Data::Serializer;

BEGIN { extends 'Catalyst::Controller'; }

use DashboardConfig qw(returnConfig);
use Dashboard::Controller::Helpers::MetricsHelper    
        qw(
            get_period_info
            get_stats_info
            get_stats4asset_type
            retrieve_thresholds
            destroy_comment_detail
            update_comment
            get_table_info
            update_thresholds
            set_thresh2default
        );

our $config    = returnConfig();
our $cachetime = $config->{ CACHE }{ time4capstat };

use DateTime;

=head1 NAME

Dashboard::Controller::Metrics - Catalyst Controller

=head1 DESCRIPTION

Catalyst Controller.

=head1 METHODS

=cut


=head2 index

  Shows the Metrics Dashboard displaying heatmap of prescribed daily, weekly and monthly metrics

=cut
 
sub index :Path :Args(0) {
    my ( $self, $c ) = @_;

    if ( $c->request->path !~ m{/$} ) {
    
        $c->response->redirect( $c->request->path . '/' );
        return 0;
    }

    $c->stash->{template} = 'objects/vdashstatus.tt2';
}


=head2 base

Base for Chained method attribute
The method will provide the header titles and core switches stats, so they are immediately available

=cut

sub base : Chained('/') PathPart('metrics/base') CaptureArgs(0) {
    my ( $self, $c ) = @_;

    $c->stash->{ 'thresh_update_allowed' }   = 
                ( exists $c->session->{ allowed }{ 'Metrics Threshold' } ) ? 1 : 0;
    $c->stash->{ 'comment_update_allowed' }   = 
                ( exists $c->session->{ allowed }{ 'Metrics Comment' } )   ? 1 : 0;

    my ( $period_info, $period_headers ) = get_period_info( $c ); 
    $c->stash->{ period_headers } = $period_headers; 
    $c->stash->{ period_info }    = $period_info; 

    $c->stash->{ anchortype_info } = {
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
                            stats_type  => 'b',    ## both
                        },
                        '5' => { 
                            anchortype  => 'logtw',
                            assettype   => 'sss',
                            assetlabel  => 'LOGICAL TOWERS',
                            stats_type  => 's',    ## summary

                        },
                     };

    ## we use the cache
##===================================================================
## FOR DEBUGGING
###=============
#$c->cache->set('assets_stats_info', undef);
##===================================================================
    my $serializer = Data::Serializer->new();
    if ( my $cacheinfo = $c->cache->get('assets_stats_info') ) {
        $c->stash->{ assets_stats_info } = $serializer->deserialize( $cacheinfo );
    }
    else {
        my $assets_stats_info            = get_stats_info( $c );
        $c->stash->{ assets_stats_info } = $assets_stats_info;
        $c->cache->set('assets_stats_info', 
        $serializer->serialize($assets_stats_info), $cachetime );
    }

}

=head2 status

the main Metrics Status Dashboard page
gets metrics stats for all datacentres

this page then gets updated by Ajax calls depending on what information the user needs further

=cut

sub status : Chained('base') PathPart('status') Args(0) {
    my ( $self, $c ) = @_;

    my $dt = DateTime->now(time_zone => 'Europe/London');

    ## choose which metrics we need if not all (depends on )
    #-----------------------------------------
    my $stats_type;
    foreach my $asset_type ( 3 .. 5 ) {
        $stats_type->{ $asset_type } = 'b';

        my $asset_stats_type = $c->stash->{ anchortype_info }{ $asset_type }{ stats_type };
        given ( $asset_stats_type ) {
    
            ## we only need bandwidth stats for the summary
            when ( 's' ) {
                $c->stash->{ assets_stats_info }{ $asset_type } = 
                        [ $c->stash->{ assets_stats_info }{ $asset_type }[0],
                          $c->stash->{ assets_stats_info }{ $asset_type }[1], ];
                $stats_type->{ $asset_type } = $asset_stats_type;
            }
            ## we do not need bandwidth stats as we already have them
            ## (detail)
            when ( 'd' ) {
                shift @{$c->stash->{ assets_stats_info }{ $asset_type }};
                shift @{$c->stash->{ assets_stats_info }{ $asset_type }};
                $stats_type->{ $asset_type } = $asset_stats_type;
            }
        } 
    } 

        ## TODO: get all the relevant stats from the csw daily/weekly/monthly stats tables 
    ## -------------------------------------------------------------------------------
    my $stats_data = { 
                        stats_type => $stats_type,
                        stats_info => $c->stash->{ assets_stats_info },
                     };

    ## CORE SWITCH stats
    #===================
    my $core_switches = get_stats4asset_type( $c, $dt, 
                                              $stats_data, 
                                              $c->stash->{ period_info },
                                              3 );

    foreach my $terr ( sort keys %$core_switches ) { 

        my $anchorids = $core_switches->{ $terr }{ anchorids };

        ## FIREWALL stats
        #================
        my $firewalls = get_stats4asset_type( $c, $dt, 
                                              $stats_data, 
                                              $c->stash->{ period_info },
                                              4, $anchorids );

        foreach my $dc ( keys %$firewalls ) {
        #--------------------------------------------------------------------------------

            my $afw_index = 0;
            foreach my $anchorid ( @{$core_switches->{ $terr }{ anchorids }} ) {

                do {
                    foreach my $key ( keys %{$firewalls->{ $dc }} ) {
                        $core_switches->{ $terr }{ anchors }[$afw_index]{ $key } = $firewalls->{ $dc }{ $key }; 
                    }
                    
                } if $dc == $anchorid;

                $afw_index++;
            }

            my $anchorids = $firewalls->{ $dc }{ anchorids };

            ## LOGICAL TOWER stats
            #=====================
            my $logtowers = get_stats4asset_type( $c, $dt, 
                                                  $stats_data, 
                                                  $c->stash->{ period_info },
                                                  5, $anchorids, 1 );

            my $midtw_index = 0;
            foreach my $midtw ( keys %$logtowers ) {

                my $midtw_index = 0;
                foreach my $anchorid ( @{$firewalls->{ $dc }{ anchorids }} ) {

                    do {
                        foreach my $key ( keys %{$logtowers->{ $midtw }} ) {
                            $firewalls->{ $dc }{ anchors }[$midtw_index]{ $key } = 
                                            $logtowers->{ $midtw }{ $key }; 
                        }
                        
                    } if $midtw == $anchorid; 
    
                    $midtw_index++;
                }
            }
        #--------------------------------------------------------------------------------
        }

    }

    $c->stash->{ message }      = 'Please wait. Loading ...';

    $c->stash->{ territory_stats }     = $core_switches;     
    $c->stash->{ template }       = 'objects/cdashstatus.tt2';

}

=head3 get_stats

Ajax call to get all Metrics stats for a datacentre midtower or midtower logical tower
gets fired when the user clicks on the midtower name or logical name or when display of all stats is requested

IN:    Controller object
    Catalyst Object
    asset type  ... sss/(afw/csw - not currently needed)
    stats_type  ... d/s/b
    anchor type ... logtw      to get stats for logtower with id = anchor_id
    midtower to get stats for logtowers of a midtower with id = anchor_id
    anchor_id   ... $logtwid/$midtwid

=cut

sub get_stats : Chained('base') PathPart('get_stats') Args(4) {
    my ($self,   $c, 
        $asset_type, $stats_type, $anchor_type, $anchor_id) = @_;

        given ( $asset_type ) {
    
            when ( 'csw' ) {
                $asset_type = 3;
            }
            when ( 'afw' ) {
                $asset_type = 4;
            }
            when ( 'sss' ) {
                $asset_type = 5;
            }
        }

    $c->stash->{ assettype }  = $asset_type;

    my $stats = {};
    my ($stats_type_href);

    if ( $stats_type eq 'd') {

        shift @{$c->stash->{ assets_stats_info }{ $asset_type }};
        shift @{$c->stash->{ assets_stats_info }{ $asset_type }};
    }

    my $stats_data = { 
               stats_type => { $asset_type => $stats_type },
               stats_info => $c->stash->{ assets_stats_info },
             };

    my $is_parent;
    if ( $asset_type == 5 && $anchor_type eq 'midtw' ) {
        $is_parent = 1;
    }    

    my $dt = DateTime->now(time_zone => 'Europe/London');

    ## LOGTOWER stats
    #===================
    $stats = get_stats4asset_type(  $c, $dt, 
                    $stats_data, 
                    $c->stash->{ period_info },
                    5, $anchor_id, $is_parent);

    $c->stash->{ stats_data } = $stats;

    $c->stash(current_view => 'JSON');
    $c->forward('View::JSON');
}

=head3 get_thresholds

Ajax call for viewing or updating a particular threshold

=cut

sub get_thresholds : Local Args(5) {
    my ($self,         $c,
        $assettypeid, $anchorid,   $modelid, 
        $metricid,    $periodname) = @_; 

    $periodname =~ s/\A(\w+)\-.*/$1/;
    $periodname = lc( $periodname );

    $c->stash->{ assettypeid }  =  $assettypeid;
    $c->stash->{ anchorid }     =  $anchorid;
    $c->stash->{ modelid }      =  $modelid;
    $c->stash->{ metricid }     =  $metricid;
    $c->stash->{ periodname }   =  $periodname;
    
    my $assetinfo = {
                assettypeid     => $assettypeid,
                anchorid        => $anchorid,
                modelid         => $modelid,
                metricid        => $metricid,
                periodname      => $periodname,
            }; 

    my $threshold = retrieve_thresholds( $c, $assetinfo ); 

    $c->stash->{ current_threshold } = $threshold->{ current }; 
    $c->stash->{ default_threshold } = $threshold->{ default };
    $c->stash->{ modelname }         = $threshold->{ modelname };
    $c->stash->{ set2default }       = $threshold->{ set2default };

    $c->forward('View::JSON');

}

=head3 set_thresholds

Ajax action for updating threshold values

=cut

sub set_thresholds : Local Args(0) {
    my ($self,   $c) = @_; 

    ## we can return as nothing changed
    ## (TODO: for lack of time done here, but should be done in metrics.js))
    my $current_changed  = 0;
    my $thresh_def_changed = 0;

    $current_changed = 1 unless
          $c->req->body_params->{ current_orange_peak } ==
          $c->req->body_params->{ orig_current_orange_peak }
                 &&
          $c->req->body_params->{ current_orange_95th } ==
          $c->req->body_params->{ orig_current_orange_95th }
                 &&
          $c->req->body_params->{ current_red_peak } ==
          $c->req->body_params->{ orig_current_red_peak }
                 &&
          $c->req->body_params->{ current_red_95th } ==
          $c->req->body_params->{ orig_current_red_95th };

    $thresh_def_changed = 1 unless
          $c->req->body_params->{ default_orange_peak } ==
          $c->req->body_params->{ orig_default_orange_peak }
                 &&
          $c->req->body_params->{ default_orange_95th } ==
          $c->req->body_params->{ orig_default_orange_95th }
                 &&
          $c->req->body_params->{ default_red_peak } ==
          $c->req->body_params->{ orig_default_red_peak }
                 &&
          $c->req->body_params->{ default_red_95th } ==
          $c->req->body_params->{ orig_default_red_95th };

    do {
        $c->stash->{ outcome } = 3;
        $c->forward('View::JSON');
    } unless $current_changed or $thresh_def_changed;  

    ## we need to update
    #-------------------

    my $assettypeid = $c->req->body_params->{ thresh_assettypeid };
    my $anchorid    = $c->req->body_params->{ thresh_anchorid };
    my $anchormodel = $c->req->body_params->{ thresh_anchormodel };
    my $metricid    = $c->req->body_params->{ thresh_metricid };
    my $metrictype  = $c->req->body_params->{ thresh_metrictype };
    my $periodname  = $c->req->body_params->{ thresh_periodname };
    my $set2default = $c->req->body_params->{ thresh_set2default };

    my ($current_orange_peak ,$current_orange_95th ,$current_red_peak ,$current_red_95th );
    my ($default_orange_peak ,$default_orange_95th ,$default_red_peak ,$default_red_95th );


     $current_orange_peak  = $c->req->body_params->{current_orange_peak };
     $current_orange_95th  = $c->req->body_params->{current_orange_95th };
     $current_red_peak     = $c->req->body_params->{current_red_peak };
     $current_red_95th     = $c->req->body_params->{current_red_95th };

     $default_orange_peak  = $c->req->body_params->{default_orange_peak };
     $default_orange_95th  = $c->req->body_params->{default_orange_95th };
     $default_red_peak     = $c->req->body_params->{default_red_peak };
     $default_red_95th     = $c->req->body_params->{default_red_95th };

    my $assetinfo = {
                assettypeid => $assettypeid,
                anchorid    => $anchorid,
                metricid    => $metricid,
                anchormodel => $anchormodel,
                metrictype  => $metrictype,
                periodname  => lc($periodname),
                set2default => $set2default || '0',
                thresh_def_changed => $thresh_def_changed,
            }; 
    
    my $threshold_data = {
                current_orange_peak => $current_orange_peak,
                current_orange_95th => $current_orange_95th,
                current_red_peak    => $current_red_peak,
                current_red_95th    => $current_red_95th,

                default_orange_peak => $default_orange_peak,
                default_orange_95th => $default_orange_95th,
                default_red_peak    => $default_red_peak,
                default_red_95th    => $default_red_95th,
                 };
    my ( $asset_abbr, $anchor ) = get_table_info( $assetinfo->{ assettypeid } );

    ## get current threshold
    ##----------------------
    my ( $update );

    ## first update the asset thresholds
    $c->stash->{ outcome } = update_thresholds($c, $assetinfo, $threshold_data);

    if ( $c->stash->{ outcome } == 1 ) {

        $c->stash->{ assettypeid } = $assettypeid;
        $c->stash->{ anchorid }    = $anchorid;
        $c->stash->{ anchormodel } = $anchormodel;
        $c->stash->{ metricid }    = $metricid;
        $c->stash->{ metrictype }  = $metrictype;
        $c->stash->{ periodname }  = $periodname;
        $c->stash->{ set2default } = $set2default;
        $c->stash->{ thresh_def_changed } = $thresh_def_changed;

        $c->stash->{ orange }{  peak }  = $c->req->body_params->{ current_orange_peak };
        $c->stash->{ orange }{ '95th' } = $c->req->body_params->{ current_orange_95th };
        $c->stash->{ red }{  peak }     = $c->req->body_params->{ current_red_peak };
        $c->stash->{ red }{ '95th' }    = $c->req->body_params->{ current_red_95th };

        if ( $thresh_def_changed ) {
            ( $c->stash->{ outcome },
              $c->stash->{ anchorids } ) = set_thresh2default( $c, { asset_abbr => $asset_abbr, 
                                                                     anchor     => $anchor }, 
                                                               $assetinfo,$threshold_data );
        }

    }

    $c->forward('View::JSON');

}

=head3 delete_comment_detail

=cut

sub delete_comment_detail : Local Args(1) {
    my ($self, $c,
        $detailid) = @_; 

    my $success = destroy_comment_detail($c, $detailid);
    
    $c->stash->{ success } = $success;
    $c->forward('View::JSON');

}

=head3 save_comment_title

=cut

sub save_comment_title : Local Args(2) {
    my ($self, $c,
        $titleid, $title) = @_;

    my $search = {
                    id       => $titleid,
                 };

    my $content   = {  title => $title };
    my $success = update_comment($c, 't', $search, $content);

    $c->stash->{ success } = $success;

    $c->stash(current_view => 'JSON');
    $c->forward('View::JSON');
}

=head3 save_comment_detail

Ajax call

=cut

sub save_comment_detail : Local Args(4) {
    my ($self, $c,
        $commentid, $detailid, 
        $customer,  $throughput) = @_;

    my $search = {
                    id => $detailid,
                 };

    my $content = { 
            comment_id => $commentid,
            customer   => $customer,
            throughput => $throughput};

    my ( $success, $newdetailid ) = update_comment($c, 'd', $search, $content);

    $c->stash->{ success }  = $success;
    $c->stash->{ detailid } = $newdetailid if $newdetailid;

    $c->stash(current_view => 'JSON');
    $c->forward('View::JSON');
}

=head1 AUTHOR

Tamara Kaufler

=cut

__PACKAGE__->meta->make_immutable;

1;
