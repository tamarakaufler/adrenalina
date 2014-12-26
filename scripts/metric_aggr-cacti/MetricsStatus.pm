package DataStatusHelper;

use strict;

=head1 DataStatusHelper

helper methods for the script for pulling out info from RRD files and inserting the relevant
info into database - used for the script gathering Data Status metrics data and calculating 
daily/weekly/monthly stats

=cut

$| = 1;

my $DEBUG = 0;
#$DEBUG = 1;

use strict;
use warnings;

#$ENV{DBIC_TRACE} = 1;

use FindBin       qw($Bin);
use lib           "$Bin/..";

use YAML::Tiny;

use RRDs;
use List::Util   qw(max min sum);
use DateTime;
use Digest::MD5  qw(md5_hex);

use POSIX        qw(sys_wait_h);

use Schema::AssetDB;
use Schema::CactiDB;
use Schema::DataStatusDB;

use Exporter;

our (@ISA)     = qw(Exporter);
our @EXPORT_OK  = qw(
            get_config
            get_assetdb_schema
            get_schemas
            get_rrd_path
            get_rrd_rules
            get_cacti_data
            afw4data_status
            csw4data_status
            logtw4data_status
            process_csw_metrics
            process_afw_metrics
            process_logtw_metrics
            asset_type2num
            table_info4asset_type
            get_date_info
            store_stats
            cleanup_old_data
    
            get_coreswitches
            get_firewalls
            get_servers
                    );

## spit out the SQL queries
## ------------------------
#$ENV{ DBIC_TRACE } = 1;

my $log_filebase = "$Bin/dataStatusLog";

## handle for core switches and firewall logging
## ---------------------------------------------
my $lhandle      = _setup_logging($log_filebase, '>'); 


=head3

=cut

sub get_config ($) {
    my ( $file ) = @_;

    die if not -e $file;

    my $yaml = YAML::Tiny->new;
        $yaml = YAML::Tiny->read( $file );
             
    my $config = {
            rrd       => $yaml->[0]->{rrd}, 
            assetdb      => { 
                    dbd     => $yaml->[0]->{ assetdb }{ dbd },
                    host    => $yaml->[0]->{ assetdb }{ host },
                    db      => $yaml->[0]->{ assetdb }{ db },
                    user    => $yaml->[0]->{ assetdb }{ user },
                    connect => $yaml->[0]->{ assetdb }{ connect },
                     },
            data_status      => { 
                    dbd     => $yaml->[0]->{ data_status }{ dbd },
                    host    => $yaml->[0]->{ data_status }{ host },
                    db      => $yaml->[0]->{ data_status }{ db },
                    user    => $yaml->[0]->{ data_status }{ user },
                    connect => $yaml->[0]->{ data_status }{ connect },
                     },
            cacti      => { 
                    dbd     => $yaml->[0]->{ cacti }{ dbd },
                    host    => $yaml->[0]->{ cacti }{ host },
                    db      => $yaml->[0]->{ cacti }{ db },
                    user    => $yaml->[0]->{ cacti }{ user },
                    connect => $yaml->[0]->{ cacti }{ connect },
                     },
                  };

    return $config;

}

=head3 get_schemas

IN:    $capstat_config ... hashref with connection details
    $cacti_config   ... hashref with connection details

=cut

sub get_schemas {
    my ( $capstat_config, $cacti_config ) = @_;

    my $capstat_schema = _getCAPSTATSchema( $capstat_config );
    my $cacti_schema   = _getCACTISchema(   $cacti_config );
    
    my $db = {
            data_status => $capstat_schema,
            cacti       => $cacti_schema,
         };
    
}

=head3 get_rrd_path

gets the rrd path where the requested information is stored

IN:    
    Cacti RRD dir
    hashref with assetdb schema and cacti schema
    arrayref with rules for finding the rrd file :
                            [ [ ldtd_id1, nc1 ], [ ldtd_id2, ncs2] ... ]
    device hashref (keys: id (cacti host id), hostname, ip)

OUT:    arrayref of rrd file paths
    if an rrd does not exist for a particular ldtd, then it will be an empty string:
                [ /.../rrd1.rrd, '', /.../rrd2.rrd, /.../rrd3.rrd, '' ]
                [ '', '', '', '' ]

=cut

sub get_rrd_path ($$$$) {
    my ( $rrd_dir, $db, $rules_ref, 
         $host, 
       ) = @_;

    my ( $search, $path_search, $ldtd_search, 
         @rrd_paths, @rrd_results, $rrd_path );    

    my ( $capstat_schema, $cacti_schema ) = ( $db->{ data_status },   $db->{ cacti } );
    $host->{ name } = _normalize_device_name( $host->{ name } );

    my @ldtd_ids = map { $_->[0] } @{ $rules_ref };

    ##========================================================================
    ## we can have metric in multiple rrds, so we need to find them all
    ## and store in an array for future summing up for a device
    ## ----------------------------------------------------------------
    my $index = 0;
    foreach my $ldtd_id ( @ldtd_ids ) {
        $search = _create_search_selection( $cacti_schema, $host, $ldtd_id );
        push @rrd_paths, '' and next if not $search;
    
        @rrd_results = $cacti_schema->resultset('DataTemplateData')
                        ->search( $search, 
                            { 'select'   => [ 'data_source_path', 'name_cache' ] })
                        ->all;

        push @rrd_paths, '' and next if not scalar @rrd_results;
        my $current_rrd = $rrd_results[0];

        if ( ( scalar @rrd_results ) > 1 ) {
            $current_rrd = _select_rrd( $rules_ref, $index, \@rrd_results );
        }

        push @rrd_paths, '' and next if not $current_rrd;

        $rrd_path = $current_rrd->data_source_path;        
        $rrd_path =~ s#<path_rra>#$rrd_dir#;
        push @rrd_paths, $rrd_path;

        $index++;
    }
    ##========================================================================

    return \@rrd_paths;
}

=head3 get_cacti_data

IN:    RRD file path
    hashref with $end_time, $delta
    dsname ... optional (provide if the RRD file contains more stored metrics)

#OUT:    arrayref of arrayrefs: [ [ (timepoint, value ) ], [ (timepoint, value ], .... ]
OUT:    hashref: {  $timepoint, $value }, { $timepoint, $value }, .... 

=cut

sub get_cacti_data ($$$) {
     my ( $rrd_file, 
          $fetch_options, $rrd_index
        ) = @_;

    my ( $start_time, $end_time,
         $resolution ) = ( 
                     $fetch_options->{ start_time }, 
                     $fetch_options->{ end_time }, 
                     $fetch_options->{ resolution }, 
                   );

    return unless $start_time && $end_time && $resolution;

    my ($start,$step,$dsnames,$data) =
                    RRDs::fetch($rrd_file, "AVERAGE", "-r", $resolution, "-s", "$start_time", "-e", "$end_time");

     my $rrd_data = _massage_rrd_data( $fetch_options, $rrd_index,
                                       $dsnames,       $data );

    return ($rrd_data, $start, $step);
}


=head3 csw4data_status

IN:    arrayref of core switches as DBIC objects
OUT:    hashref  of core switches associated with territories and datacentres
            { territory_id }{ datacentre_id }{ ciID } = {     name => name,
                                    ip   => ip 
                                    }

=cut

sub csw4data_status {
    my ($csw_ref) = @_;

    my %csw = ();

    foreach my $device ( @$csw_ref ) {

        my $device_name = $device->name;

        next unless $device_name =~ /\-[012]01\-core/        or $device_name =~ /\-[012]02\-core/;
        next if ($device_name =~ /xxx/i or $device_name =~ /yyyy/i) 
                                        && 
                 $device_name =~ /csw\-/i;

        $csw{ $device->get_column('terrid') }
            { $device->get_column('dcid') } 
            { $device->ciID } = {
                        name   => $device_name,
                        ip     => $device->ip,
                        dcname => $device->get_column('dcname'),
                    }; 
        
    }

    return \%csw;
}

=head3 afw4data_status

IN:    arrayref of firewalls as DBIC objects
OUT:   hashref  of firewalls associated with datacentres and midtowers
            { datacentre_id }{ midtower_id } = {     name => name,
                                ip   => ip 
                               }
=cut

sub afw4data_status {
    my ($afw_ref) = @_;

    my %afw = ();

    foreach my $device ( @$afw_ref ) {

        $afw{ $device->get_column('dcid') }
            { $device->parent_id } = {
                        name   => $device->name,
                        ip     => $device->ip,
                        dcname => $device->get_column('dcname'),
                    }; 
    }

    return \%afw;
}

=head3 logtw4data_status

IN:    hashref of logical tower with arrayref of their scanning servers as DBIC objects
OUT:   hashref  of firewalls associated with datacentres and midtowers
            { datacentre_id }{ midtower_id }{ ciID } = {     name => name,
                                    ip   => ip 
                                   }

=cut

sub logtw4data_status {
    my ($srv_ref) = @_;

    my %srv = ();

    foreach my $device ( @$srv_ref ) {
        my $device_name   = $device->name;
        my $device_parent = $device->parent_id;

        ## we are only interested in scanning servers
        next unless $device_name =~ /\s*sss\-\d+\-t.*/;
        next unless $device_parent and $device_parent != 0 and $device_parent != -1;


        $srv { $device->get_column('terrid') }
             { $device->get_column('dcid') }
             { $device->parent_id }
             { $device->ciID } = {
                                    name   => $device_name,
                                    ip     => $device->ip,
                                    dcname => $device->get_column('dcname'),
                                 }; 
    }

    return \%srv;

}

=head3 _log_missing_rrd_file

IN:     log filehandle
        hostname
        metric id
=cut

sub _log_missing_rrd_file ($$$) {
    my ( $fhandle, $host, $metric) = @_;

    return if not $fhandle;

    my $hostname = $host->{ name };
    my $ip       = $host->{ ip };

    print $fhandle 
        "\n\t>> ERROR: rrd file MISSING: ERROR  $hostname ($ip)\nmissing rrd file for metric " .  $metric . " <<\n\n";

}

=head3 process_csw_metrics

process Cacti RRD data for provided devices

IN:     Cacti RRD dir
        a hashref containing CMDB and Cacti schemas
        RRD info needed for fetching rrd data:

        RRD rules hashref  ... for selecting RRD file containing a specific metric
        RRD times hashref for the fetch method { end_time, delta } ... collecting data
        #metrics resolution ... determines what granularity from the RRD file will be selected

        hashref of core switches

=cut

sub process_csw_metrics ($$$$) {
    my ($rrd_config, $db, 
        $rrd_info,
        $csw_ref) = @_;

    my $lhandle = _setup_logging($log_filebase, '>', 'csw');
    my ( $capstat_schema, $cacti_schema ) = ( $db->{ data_status }, $db->{ cacti } );
    my ( $rrd_cache, $rrd_cache_time, $rrd_dir ) = ( 
		                            $rrd_config->{ cache },
		                            $rrd_config->{ cache_time },
		                            $rrd_config->{ dir },
		                                  );
    my ( $rule_ref, $rrd_time_ref ) = ( 
                                            $rrd_info->{ rule }, 
                                            $rrd_info->{ time }, 
                                      );

    foreach my $terrid ( keys %$csw_ref ) {
        my $dc_csw_rrd = {};

        my %terr_dc = %{$csw_ref->{ $terrid }};
        foreach my $dcid ( keys %terr_dc ) {

            my %dc_csw = %{ $csw_ref->{ $terrid }{ $dcid } };
            foreach my $cswid ( keys %dc_csw ) {
    
                my $host = { name => lc($dc_csw{ $cswid }{ name }), 
                             ip   => $dc_csw{ $cswid }{ ip } };
                my $dcname = $dc_csw{ $cswid }{ dcname };

                ## getting metrics
                ## ---------------
                ## cycle through all the metrics
                ## -----------------------------
                my $asset_info = {
                                    db         => $db, 
                                    lhandle    => $lhandle, 
                                    host       => $host, 
                                    rule_ref   => $rule_ref, 
                                    rrd_cache  => $rrd_cache, 
                                    rrd_dir    => $rrd_dir, 
                                    stats_rule => $stats_rule_ref,
                                    asset      => $dcid, 
                                 };
                _get_rrd_paths4asset($asset_info, $dc_csw_rrd);
            }
        }

        store_metrics($capstat_schema, 3, $rrd_info, $dc_csw_rrd, $terrid);
    }

}

=head3 process_afw_metrics

process Cacti RRD data for provided devices

IN:    Cacti RRD dir
    a hashref containing CMDB and Cacti schemas
    hashref of core switches

=cut

sub process_afw_metrics {
    my ($rrd_config, $db, 
        $rrd_info,
        $afw_ref) = @_;

    my $lhandle = _setup_logging($log_filebase, '>', 'afw');

    my ( $capstat_schema, $cacti_schema ) = ( $db->{ data_status }, $db->{ cacti } );

    my ( $rrd_cache, $rrd_cache_time, $rrd_dir ) = ( 
                                            $rrd_config->{ cache },
                                            $rrd_config->{ cache_time },
                                            $rrd_config->{ dir },
                                                   );
    my ( $rule_ref, $rrd_time_ref ) = ( 
                                            $rrd_info->{ rule }, 
                                            $rrd_info->{ time }, 
                                      );

    foreach my $dcid ( keys %$afw_ref ) {
        my $dc_afw_rrd = {};

        my %dc_afw = %{ $afw_ref->{ $dcid } };
        foreach my $logtw ( keys %dc_afw ) {

            my $host = { name => $dc_afw{ $logtw }{ name }, 
                         ip   => $dc_afw{ $logtw }{ ip }};
            my $dcname = $dc_afw{ $logtw }{ dcname };

            ## getting metrics
            ## ---------------
            ## cycle through all the metrics
            ## -----------------------------
            my $asset_info = {
                                db         => $db, 
                                lhandle    => $lhandle, 
                                host       => $host, 
                                rule_ref   => $rule_ref, 
                                rrd_cache  => $rrd_cache, 
                                rrd_dir    => $rrd_dir, 
                                stats_rule => $stats_rule_ref,
                                asset      => $logtw, 
                             };
            _get_rrd_paths4asset($asset_info, $dc_afw_rrd);
        }

        store_metrics($capstat_schema, 4, $rrd_info, $dc_afw_rrd);
    }


}

=head3 process_logtw_metrics

process Cacti RRD data for provided devices

IN:    
    Cacti RRD dir
    a hashref containing CMDB and Cacti schemas
    rrd info: rules for finding the RRD files for specific asset_type/metric combinations
    hashref of logtower servers
    
    parallelize ... optional : undef/0 or 1/string (process datacentres in forks)

=cut

sub process_logtw_metrics ($$$$;$) {
    my ($rrd_dir,  $db, 
        $rrd_info, $srv_ref,
        $parallelize
        ) = @_;

    print "*** Start of process_logtw_metrics sub - "  . scalar localtime(time) . "\n" if $DEBUG;

    my ( $capstat_schema, $cacti_schema ) = (  $db->{ data_status }, 
                        $db->{ cacti }, 
                         );
    my ( $rule_ref, $rrd_time_ref    ) = ( 
                        $rrd_info->{ rule }, 
                               $rrd_info->{ time }, 
                         );
    ## Loop through territories
    ## ------------------------
    foreach my $terrid ( keys %$srv_ref ) {

        print "\t===> LOGTW territory $terrid STARTed processing at " . scalar localtime(time) . "\n"
                                        if $DEBUG;

        my $lhandle = _setup_logging($log_filebase, '>', 'logtw', "${terrid}");

        foreach my $dcid ( keys %{$srv_ref->{ $terrid }} ) {
            _process_datacentre_logtowers( $lhandle, 
                               $rrd_dir, $db, $rrd_info,
                               $terrid,  $dcid,
                               $srv_ref->{ $terrid }, $parallelize );
        }

        print "\t===> LOGTW territory $terrid ENDed at " . scalar localtime(time) . "\n" if $DEBUG;
    }

    print "*** End of process_logtw_metrics sub - "  . scalar localtime(time) . "\n\n" if $DEBUG;


}

=head3 asset_type2num

IN:     device type: dsw/afw/logtw
OUT:    numerical equivalent

=cut

sub asset_type2num {
    my $type = @_;
    
    if ( $type eq 'csw' ) {
        return 22;
    }
    elsif ( $type eq 'afw' ) {
        return 33;
    }
    ## this is an oddball: logical tower has clFK=1, but it is not a physical device, 
    ## so we use clFK=5 for servers that are the monotored devices and are children of a logical tower
    elsif ( $type eq 'logtw' ) {
        return 44;
    }

    return $type if $type =~ /\A\d+\z/;
    return;
}

=head3 store_metrics

gets the raw rrd data, processes it and stores the required metrics for given device type

IN:    Cmdb schema 
    asset type: 3/4/5
    RRD time factors for fetching rrd data: end_time and delta
    hashref with all rrd files per datacentre and stats type

    Territory id (optional. used when logging logical tower metrics)

=cut

sub store_metrics ($$$$;$) {
    my ( $capstat_schema, 
         $asset_type, $rrd_info_ref, $rrd,
         $terrid ) = @_;
    
    my $rrd_data    = _get_metrics( $capstat_schema, $asset_type, 
                        $rrd_info_ref,
                        $rrd );
    my $metric_href = _process_metrics($rrd_info_ref->{ rule }, $rrd_data);

    _store_metrics(    $capstat_schema, $asset_type, $metric_href, $terrid);
}

=head3 get_rrd_rules

gets all rules from the database how to find 

IN:     CMDB schema
OUT:    hashref : { stats_id } = [ [ ldtd_id,   name_cache,     dsname,         resolution,     action ],
                   [ ldtd_id,   name_cache,     dsname,         resolution,     action ],
                    ... 
                    ... 
                 ]  

=cut

sub get_rrd_rules {
    my ( $capstat_schema ) = @_;
    
    my $rules = {};
    my @rows = $capstat_schema->resultset('DataStatusRrdInfo')->search({})->all;

    foreach my $row ( @rows ) {

        ## for efficiency reasons using arrayref rather than the more readable hashref
        ## [ ldtd_id,     name_cache,     dsname,     resolution,     action ]

        my $rule_info = [ $row->local_data_template_data_id, $row->name_cache,
                  $row->dsname,              $row->resolution,    
                  $row->action,              $row->id ];

        push @{ $rules->{ $row->asset_type }{ $row->metric } }, $rule_info; 

    }

    return $rules;
}

=head3 table_info4asset_type

provides table names and anchor column name for an asset type

IN:     asset type 3/4/5
        territory id (optional)
OUT:    array of table info hashref and a anchor column name

=cut

sub table_info4asset_type ($;$) {
    my ( $asset_type, $terrid ) = @_;

    my ( $anchor_field, $metrics_table );

    if ( $asset_type == 3 ) {
        $anchor_field = 'datacentre';
        $metrics_table->{ timepoint } = 'DataStatusTimepointSw';
        $metrics_table->{ daily }   = 'DataStatusDailySw';
        $metrics_table->{ weekly }  = 'DataStatusWeeklySw';
        $metrics_table->{ monthly } = 'DataStatusMonthlySw';
    }
    elsif ( $asset_type == 4 ) {
        $anchor_field = 'midtower';
        $metrics_table->{ timepoint } = 'DataStatusTimepointFw';
        $metrics_table->{ daily }   = 'DataStatusDailyFw';
        $metrics_table->{ weekly }  = 'DataStatusWeeklyFw';
        $metrics_table->{ monthly } = 'DataStatusMonthlyFw';
    }
    elsif ( $asset_type == 5 ) {
        $terrid ||= '';
        $anchor_field = 'logtower';
        $metrics_table->{ timepoint } = "DataStatusTimepointLogtw$terrid";
        $metrics_table->{ daily }   = 'DataStatusDailyLogtw';
        $metrics_table->{ weekly }  = 'DataStatusWeeklyLogtw';
        $metrics_table->{ monthly } = 'DataStatusMonthlyLogtw';
    }

    return ($anchor_field, $metrics_table);

}

=head3 get_date_info

get current time and create a hashref with current date info

IN:    N/A
OUT:     array of an href with stored year/month/week/day and DateTime object

=cut

sub get_date_info {
    my ( $dt,  
         $year, $month, $week, 
         $day );

    $dt  = DateTime->now();

    $month = $dt->month;
    ($year, $week)  = $dt->week;
    $day   = $dt->day_of_year;

    my $date_href = { 
                    year  => $year,
                     month => $month,
                     week  => $week,
                    day   => $day,
                };

    return ( $date_href, $dt );

}

## ------------------------------------------- PRIVATE METHODS ---------------------------------------------

=head _getCAPSTATSchema

=cut

sub _getCAPSTATSchema {
    my ( $connect_data ) = @_;

    my ( $capstat_dbd, 
         $capstat_host, 
         $capstat_db, 
         $capstat_user, 
         $capstat_connect ) = (
                    $connect_data->{ dbd },
                    $connect_data->{ host },
                    $connect_data->{ db },
                    $connect_data->{ user },
                    $connect_data->{ connect },
                  );
    
         my $capstat_schema = Schema::DataStatusDB->connect(
                        "DBI:$capstat_dbd:host=$capstat_host;dbname=$capstat_db", 
                             $capstat_user, $capstat_connect)
                    or die "Cannot connect to DataStatus database: $!";
}

sub _getCACTISchema {
    my ( $connect_data ) = @_;

    my ( $cacti_dbd, 
         $cacti_host, 
         $cacti_db, 
         $cacti_user, 
         $cacti_connect ) = (
                    $connect_data->{ dbd },
                    $connect_data->{ host },
                    $connect_data->{ db },
                    $connect_data->{ user },
                    $connect_data->{ connect },
                  );

    my $cacti_schema = Schema::CactiDB->connect("DBI:$cacti_dbd:host=$cacti_host;dbname=$cacti_db", 
                                   $cacti_user, $cacti_connect)
                            or die "Cannot connect to Cacti database: $!";
}

=head3 _setup_logging

IN:    
    $log_filebase 
    $for   ......... make it clear what asset type we are logging for ('csw_afw/logtw' etc)
    $type  ......... '>', '>>'
    $id    ......... optional, for which territory/datacentre or both (4_35, 4 etc)

=cut

sub _setup_logging {
    my ( $log_filebase, $type, 
         $for, $id ) = @_;

    my $identifier = ( $for ) ? "_${for}"           : '';
    $identifier    = ( $id )  ? "${identifier}_${id}" : $identifier;

    print "LOG FILE : ${log_filebase}${identifier}.log \n\n" if $DEBUG;
    open my $fhandle, $type, "${log_filebase}${identifier}.log" or die "Cannot open a log file: $!"; 

    return $fhandle;
}

=head3 _get_rrd_paths4asset

gathers all rrd paths attributing to a particular metric
uses cached rrd path or caches if a cache miss
logs missing rrd files

IN:     hashref with config info
        asset to be processed
        hashref storing processed data

=cut

sub _get_rrd_paths4asset {
    my ($info, $rrd_path) = @_;

    my $db          = $asset_info->{ db }; 
    my $lhandle     = $asset_info->{ lhandle };
    my $host        = $asset_info->{ host };
    my $rule_ref    = $asset_info->{ rule_ref };
    my $rrd_cache   = $asset_info->{ rrd_cache };
    my $rrd_dir     = $asset_info->{ rrd_dir };
    my $stats_rule  = $asset_info->{ stats_rule };
    my $asset       = $asset_info->{ asset };
     
    foreach my $stats ( sort keys %$rule_ref ) {
        my $stats_rule_ref = $rule_ref->{ $stats };

        my ( $rrd_path_arref, $cache_key );
        $host->{ name } = _normalize_device_name( $host->{ name } );

        if ( $host->{ ip } ) {
            $cache_key = md5_hex( $host->{ name } . $host->{ ip } . $stats );
    
            if ( not $rrd_cache->is_valid( $cache_key ) ) {
                $rrd_path_arref = get_rrd_path( $rrd_dir, $db, 
                                                $stats_rule_ref,
                                                $host);
                $rrd_cache->set( $cache_key, $rrd_path_arref);
            }
            else {
                $rrd_path_arref = $rrd_cache->get( $cache_key );
            }
    
            push @{ $rrd_path->{ $asset }{ $stats } }, $rrd_path_arref;
            _log_missing_rrd_file($lhandle, $host, $stats) 
                            if _is_rrd_missing($rrd_path_arref, $lhandle);
        }
        else {
            print $lhandle "ERROR: " . $host->{ name } . " is missing IP address\n" if $lhandle;
        }
    }
}

=head3 _process_datacentre_logtowers

IN:    $rrd_dir, $db, $rrd_info,$terrid,  $dcid,$srv_terr_ref, $parallelize

=cut

sub _process_datacentre_logtowers {
    my ($lhandle, $rrd_dir, $db, $rrd_info,
        $terrid,  $dcid,
        $srv_terr_ref, $parallelize ) = @_;


    my $use = ( $parallelize ) ? '' : 'NO';
    print $lhandle "\t\t===> LOGTW $use FORK for territory $terrid/datacentre $dcid STARTed " . 
             scalar localtime(time) . "\n" if $DEBUG && $lhandle;

    if ( $parallelize && $terrid == 5 ) {
        my $child = _fork4datacentre_logtowers( 
                                $rrd_dir, $db, $rrd_info,
                                $terrid,  $dcid,
                                $srv_terr_ref,
                                $lhandle );
    }
    else {
        _process_logtw_metrics4datacentre( 
                               $rrd_dir, $db, $rrd_info,
                               $terrid,  $dcid, 
                               $srv_terr_ref, $lhandle );
    }
}

=head3 _fork4datacentre_logtowers

creates a child process for processing a datacentre in a logical tower
reaps dead children

=cut

sub  _fork4datacentre_logtowers {
    my ( $rrd_dir, $db, $rrd_info,
         $terrid,  $dcid,
         $srv_terr_ref,
         $lhandle ) = @_;

    my $child = {};

    my $pid = fork();
    if ( not defined $pid ) {
        die "Cannot fork for territory $terrid, datacentre $dcid
             (_process_datacentre_logtowers)";
    }
    elsif ( $pid ) {
        $child->{ $pid } = "$terrid-$dcid";
    }
    else {
        _process_logtw_metrics4datacentre( 
	                       $rrd_dir, $db, $rrd_info,
	                       $terrid,  $dcid, 
	                       $srv_terr_ref, 
	                       $lhandle );
        exit;
    }

    _reap_dead_children($child);

    return $child;
}

=head3 _reap_dead_children

IN:    hashref of all child process pids

=cut

sub _reap_dead_children {
    my ($child) = @_;

    while ( keys %$child ) {
        foreach my $pid ( keys %$child ) {
            my $result = waitpid $pid, WNOHANG;
            do {
                delete $child->{ $pid };
                print "\t>>> Discarded $pid for " . $child->{ $pid } . "\n" if $DEBUG;
            } if $result == $pid;
            sleep 1;
        }
    }

}

=head3 _process_logtw_metrics4datacentre

stores 5-min metrics values for a given datacentre

IN: $rrd_dir    base dir of RRD file
    $db         database connections
    $rrd_info   rules and time info for getting the correct RRD file for each metric and
                storing its data
    $terrid     territory id
    $dcid       datacentre id
    $srv_dc     servers in datacentre
OUT:    NA

=cut

sub _process_logtw_metrics4datacentre ($$$$$$;$) {
        my ( $rrd_config, $db, $rrd_info, 
         $terrid,  $dcid, $srv_dc,
         $lhandle ) = @_;

    my ( $rrd_cache, $rrd_cache_time, $rrd_dir ) = ( 
		                            $rrd_config->{ cache },
		                            $rrd_config->{ cache_time },
		                            $rrd_config->{ dir },
		                         );
    my ( $rule_ref, $rrd_time_ref    ) = (  $rrd_info->{ rule }, 
                                            $rrd_info->{ time }, 
                                         );
    my $dc_logtw_rrd = {};
    
    print $lhandle "\t\t\t\tStarting collecting from RRDs for DATACENTRE $dcid:       " .  
                                            scalar localtime() . "\n" if $DEBUG && $lhandle;

    my %logtw_dc = %{$srv_dc->{ $dcid }};
    foreach my $logtw ( keys %logtw_dc ) {

        my %logtw_srv = %{ $logtw_dc{ $logtw } };
        foreach my $srvid ( keys %logtw_srv ) {

            my $host = { name => $logtw_srv{ $srvid }{ name }, 
                         ip   => $logtw_srv{ $srvid }{ ip }};
            my $dcname = $logtw_srv{ $srvid }{ dcname };
    
            ## getting metrics
            ## ---------------
            ## cycle through all the metrics
            ## -----------------------------
            my $asset_info = {
                                db         => $db, 
                                lhandle    => $lhandle, 
                                host       => $host, 
                                rule_ref   => $rule_ref, 
                                rrd_cache  => $rrd_cache, 
                                rrd_dir    => $rrd_dir, 
                                stats_rule => $stats_rule_ref,
                                asset      => $logtw, 
                             };
            _get_rrd_paths4asset($asset_info, $dc_logtw_rrd);
        }
    }
    print $lhandle "\t\t\t\tFinished collecting from RRDs for datacentre $dcid:       " .  scalar localtime() . "\n\n"
                                                                        if $DEBUG && $lhandle;
    
    print $lhandle "\t\t\t\t=== Start storing metrics for $dcid:       " .  scalar localtime() . "\n"   
                                                                        if $DEBUG && $lhandle;

    store_metrics($db->{ data_status }, 5, $rrd_info, $dc_logtw_rrd, $terrid);

    print $lhandle "\t\t\t\t=== Finished storing metrics for $dcid:       " .  scalar localtime() . "\n"
                                                                        if $DEBUG && $lhandle;
}

=head3 _is_rrd_missing

=cut

sub _is_rrd_missing {

        my ($rrd_path_arref, $fhandle) = @_;

        my @not_empty = grep { $_ } @$rrd_path_arref;
        my @exists    = ();

        if ( $fhandle ) {
                for my $rrd_file (@not_empty) {
                        print $fhandle "\t\tERROR [$rrd_file]\n\t\tfound in cacti db, DOES NOT exist in rrd dir\n"
                                                                                                         unless -e $rrd_file;
                        push @exists, 1  if -e $rrd_file;
                }    
        }    
        else {
                for my $rrd_file (@not_empty) {
                        print "\t\tERROR [$rrd_file]\n\t\tfound in cacti db, DOES NOT exist in rrd dir\n"
                                                                                                         unless -e $rrd_file;
                        push @exists, 1  if -e $rrd_file;
                }    
        }    

        ( (scalar @not_empty) and (scalar @not_empty == scalar @exists) ) ? return 0 : return 1;

}

sub _find_or_create_metric_entry {
    my ($db_access, 
        $timepoint_table, $anchor_field, $anchor_id, $metric_id,
        $timevalue ) = @_;

    my $dt = DateTime->from_epoch( epoch => $timevalue->[0] );

    my $month = $dt->month;
    my ( $year, $week )  = $dt->week;
    my $day   = $dt->day_of_year;

    my $record = $db_access->resultset( $timepoint_table )->find_or_create(
        {
          $anchor_field     => $anchor_id,
          metric            => $metric_id,
          year              => $year,
          month             => $month,
          week              => $week,
          day               => $day,
          timepoint         => $timevalue->[0],
          value             => $timevalue->[1],
        },
        { key => 'uniq_tmpt' },
    );

}

=head3 _summarize_rrd_data

summarizes values of constituent rrd data values (from the individual metrics that make up the final
required metric)

IN:    arrayref of raw cacti data: 
              [ 
             [ val_a1, val_a2, val_a3 .... ],
             [ val_b1, val_b2, val_b3 .... ],
            ... 
              ] 
OUT:    arrayref of added contrituent raw cacti data: 
              [ 
            (val_a1 + val_b1 + ...), (val_a2 + val_b2 + ...), () ...     
              ] 

=cut

sub _summarize_rrd_data {
    my ( $rrd_data ) = @_;

    my @sum_rrd_data;

    foreach my $constituent ( @$rrd_data ) {
        next if not ref( $constituent ); 

        my $index = 0;
        foreach my $metric_value ( @$constituent ) {
            $sum_rrd_data[$index] = ( defined $sum_rrd_data[$index] ) ?  $sum_rrd_data[$index] : 0;

            $metric_value = ( defined $metric_value && $metric_value =~ /\A[0-9,.]+\z/ ) ?  $metric_value : 0;    
            $sum_rrd_data[$index] += $metric_value;
            $index++;
        }
    }

    return \@sum_rrd_data;

}

=head3 _create_search_selection

gets all collected metrics for a given host

IN:     $cacti_schema
        $host
        $ldtd_id ... local_data_template_data_id column from Cacti data_tempate data table
OUT:    DBIC search hashref

=cut

sub _create_search_selection {
    my ( $cacti_schema, $host, $ldtd_id ) = @_;

    ## get the host_id for the given ip_address from cacti database
    my $host_id = _get_host_id($cacti_schema, $host);

    my ( $hostname, $ip ) = ( $host->{ name }, $host->{ ip } );

    ## we have a problem
    ## -----------------
    do {
        print $lhandle "\tERROR: $hostname ($ip)\n\t\tcannot be found in Cacti\n";
        return;

    } if not $host_id;

    ## if the hostname appears
    $hostname =~ s/\./_/g;

    my $path_search = [
                    '-and' => [ 
                        '-or' => [ 
                                    data_source_path => {   like      => "%$hostname%" },
                                    data_source_path => {   like      => "%$ip%" },
                                 ],
                         data_source_path => { '-not_like' => '%old%' },
                      ],
                    '-or'  => [
                                    data_source_path => { like  => "%/$host_id/%" },
                                    data_source_path => { like  => "%/$ip/%" },
                              ],
                  ];

    my $search     = { 
                    '-or'                         => $path_search,
                    'local_data_template_data_id' => $ldtd_id, 
                   };

}

=head3 _select_rrd

in some cases there are multiple RRD files found for a given metric for an asset type. In this case we 
need to select the correct one based on matching the metric description in name_cache 

IN:     $rules_ref ... rules for the given metric
        $index       ... index
OUT:    the correct rrd for the given metric

=cut

sub  _select_rrd {
    my ( $rules_ref, $index, $rrds_ref ) = @_;

    my $current_rrd;
    my @ncs = split /\|/,  $rules_ref->[$index][1];


    foreach my $rrd ( @$rrds_ref ) {
        my $name_cache = $rrd->name_cache;
        my $data_source_path = $rrd->data_source_path;

        foreach ( @ncs ) {
            do {
                $current_rrd = $rrd; 
                last;
               } if $rrd->name_cache =~ /.*$_.*/i;
        }
    }

    return $current_rrd;
}

=head3 _massage_rrd_data

IN:    hashref with start_time, resolution, dsname
    $dsnames arrayref ... from RRDs::fetch
    $data    arrayref ... from RRDs::fetch

OUT:    arrayref: [ [ $timepoint, $value ], [ $timepoint, $value ], .... ]

=cut

sub _massage_rrd_data ($$$$) {
     my ( $fetch_options, $rrd_index, 
          $dsnames, $raw_data ) = @_;

     my ( $start_time, $step, $dsname ) = ( 
                     $fetch_options->{ start_time }, 
                     $fetch_options->{ resolution }, 
                     $fetch_options->{ dsname }[$rrd_index], 
                   );

     return {} unless $start_time && $step;
    
     my $massaged_data = {};

     ## in which position is the dsname, ie which data value is the one we want
     my $ds_index   = 0;
     $ds_index      = _get_ds_index( $dsnames, $dsname ) if $dsname;
     $massaged_data = _get_dsname_data($start_time, $step, $ds_index, $raw_data);

     return $massaged_data;

}

=head3 _get_ds_index 

IN:    $dsnames ... arrayref of dsnames from RRDs::fetch
    $dsname  ... dsname associated with a metric

OUT:    index in the dsnames array of the dsname element
=cut

sub _get_ds_index {
    my ($dsnames, $dsname) = @_;

     return 0 if ! $dsname;

     my $ds_index = 0;
     foreach my $ds ( @$dsnames ) {
        last if $ds eq $dsname;
        $ds_index++;
     }

     return $ds_index;
}

=head3 _get_dsname_data

IN:    $start_time, $ds_index, $raw_data
OUT:    massaged data arrayref: 

=cut

sub _get_dsname_data ($$$$) {

     my ($start_time, $step, $ds_index, $raw_data) = @_;

     my $massaged_data = {};
     my @massaged_data = ();

     my $timepoint = $start_time;
     for my $data4timepoint (@$raw_data) {

        next if ! $data4timepoint->[0];
        
        my $index = 0;
           for my $val (@$data4timepoint) {
                $val = 0 unless defined $val and $val =~ /\A[0-9.,]+\z/;

                if ( $index == $ds_index) {
                    push @massaged_data, $val;
                }
                $index++;
           }
           $timepoint += $step;
     }

     return \@massaged_data;
}

=head3 _get_host_id 

gets cacti host_id for a device given its IP address

IN:     cacti schema
        IP address
OUT:    host id

=cut

sub _get_host_id ($$) {
    my ($cacti_schema, $host) = @_;

    my $host_id;
    my ( $hostname, $ip ) = ( $host->{ name }, $host->{ ip } );

    ## using search to prevent problems if more devices have (incorrectly set) equal ip addresses
    my @rows = $cacti_schema->resultset('Host')->search( { hostname    => $ip,
                                                           description => { '-not_like' => "%old%" } },
                                                         { 'select' => [ 'id' ] })->all;
    $host_id = $rows[0]->id if scalar @rows;

    return $host_id;
    
}

=head3 _get_95th
gets 95th percentile

IN:     data arrayref ... only metric values [ qw value1 value2 value3 ... ]
        start of the time period in epoch time
        end  of the time period in epoch time

=cut

sub _get_95th ($) {
    my ( $data_arrayref ) = @_;

    return 0 unless scalar @$data_arrayref;
    return $data_arrayref->[0] if @$data_arrayref == 1;

    my $period = 100;
    my $time_steps = @$data_arrayref - 1;
    my $time_step  =  $period/$time_steps;
    my $time4perc  =  $period * 0.05;
    
    my $elapsed = 0;
    my $percentile;
    foreach my $value ( @$data_arrayref ) {
        $percentile = $value;
        last if $elapsed > $time4perc;

        $elapsed += $time_step;
    }

    return $percentile;

}

=head3 _get_metrics

IN:     $capstat_schema
        $asset_type  ... 3/4/5
        $rrd_info_ref... { time }{ end_time => ..., delta => ... }
                 { rule }{ $metric1 }{ dsname }     = [ ... ]
                     { $metric1 }{ resolution } = [ ... ]

        $rrd_file .......... 
        arrayref with rrd files:  

        example for core switches:
            $dc_csw_rrd->{ $datacentre }{ $metric } = 
                [ [rrd_file1, rrd_file2 , ... ], ... multiple rrd files if a metric ifor an asset is a sum of values 
                                                     in different rrd files
                [rrd_file1, rrd_file2 , ... ], 
                [rrd_file1, rrd_file2 , ... ], 
                ]
                    for given $asset_type

OUT:    hashref with datacentre related metrics

=cut

sub  _get_metrics {
    my ($capstat_schema, $asset_type, 
        $rrd_info_ref, $rrd_file ) = @_;

    my ( $metric_data );

    ## get the RRD data 
    ## ================
    foreach my $anchor ( keys %$rrd_file) {
        
        ## set the time interval for data collection
        ## -----------------------------------------

        ## we can be collecting metrics from multiple devices and then processing 
        ## the results based on 'action' in DataStatusRrdInfo
        foreach my $metric ( keys %{$rrd_file->{ $anchor }} ) {

            my ($start_time, $resolution);

            my $rrd_files = $rrd_file->{ $anchor }{$metric};
            my $fetch_options = _fetch_options($metric, $rrd_info_ref);

            my $cacti_data = [];

            ## Cycling through each device
            ## ----------------------------
            my $i = 0;
            foreach my $rrd_paths ( @$rrd_files ) {

                ## Cycling through each constituent rrd file for this device and metric
                ## --------------------------------------------------------------------
                my $cacti_data_ind;     # data for constituent metrics
                my $rrd_index = 0;    # position of each constituent metric

                foreach my $rrd_path ( @$rrd_paths ) {
                    my $data;
                    ## avoiding a situation when rrd fetch does not provide start_time
                    my $start_time_current = ( $start_time ) ? $start_time : undef;

                    ($data, $start_time, $resolution) = get_cacti_data($rrd_path, $fetch_options, $rrd_index);
                    $start_time = ( $start_time ) ? $start_time : $start_time_current;
                    
                    push @$cacti_data_ind, $data;
                    $rrd_index++;
                }
                ## -------------------------------------------------------------------

                ## arrayref of arrayrefs: [ [v1a, v1b, ...], [v2a, v2b, ...], ... ]
                my $sum_cacti_data_ind = _summarize_rrd_data($cacti_data_ind);  
                push @$cacti_data, $sum_cacti_data_ind;        #=> [ [] , [], ...]              

                $i++;

            }

            ## rearrange the data to have { $timepoint } = [ qw( value1 valu2 value3 ... ) ]

            $start_time = ( $start_time ) ? $start_time : $fetch_options->{ start_time };
            $resolution = ( $resolution ) ? $resolution : $fetch_options->{ resolution };
            $metric_data->{ $anchor }{ $metric } = 
                            _finalize_rrd_data($start_time, $resolution, $cacti_data);
        }    
    }


    return $metric_data;
}

=head3 _finalize_rrd_data

rearranges data to  have { $timepoint } = [ qw( value1 value2 value3 ... ) ]

IN: start_time ... RRD start time
    resolution
    data ......... arrayref of arrayrefs
OUT:    hashref: { $timepoint } => [ qw( value1 value2 value3 ... ) ]

=cut

sub _finalize_rrd_data {
    my ( $start_time, $resolution, $data ) = @_;

    return {} if not scalar @$data;

    my $rearranged_data = {};
    foreach my $device ( @$data ) {

        next if not ref( $device );     ## empty string

        my $timepoint = $start_time;
        foreach my $device_value ( @$device ) {
            push @{ $rearranged_data->{ $timepoint } }, $device_value; 
            $timepoint += $resolution;
        }

    }
    return $rearranged_data;
}

=head3 _fetch_options

IN:    
    $asset_type ... 3/4/5
    $rrd_data .......... hashref with rrd files: {$}

=cut

sub _fetch_options ($$$) {
    my ( $metric, $rrd_info_ref ) = @_;

    my ( $end_time, $delta ) = ( $rrd_info_ref->{ time }{ end_time },
                     $rrd_info_ref->{ time }{ delta } );

    my ( @dsnames );
    my $resolution = $rrd_info_ref->{ rule }{ $metric }[0][3];
    @dsnames = map { $_->[2] } @{ $rrd_info_ref->{ rule }{ $metric } }; 

    $end_time = int( $end_time/$resolution ) * $resolution;
    $delta    = int( $delta/$resolution )    * $resolution;

    my $start_time = $end_time - $delta;

    my $fetch_options = {
                            start_time => $start_time, 
                            end_time   => $end_time, 
                            resolution => $resolution,
                            dsname     => \@dsnames,
                        };

    return $fetch_options;
}

=head3 _process_metrics

calculates the final values that will be stored in the daily/weekly and monthly tables: peak (for all) 
                                                and 
                                            95th percentile
                                            (for weekly and monthly tables)
IN:     rrd rule
        rrd data .......... hashref with rrd files: { $datacentre }{ $metric } = [ {}, {} ]
OUT:    processed data

=cut

sub _process_metrics ($$) {
    my ( $rrd_rule,  $rrd_data) = @_;

    my ( $metric_data, $processed_data );

    ## process the data 
    foreach my $anchor ( keys %$rrd_data ) {

        foreach my $metric ( keys %{ $rrd_data->{ $anchor } } ) {

            my $metric_action = $rrd_rule->{ $metric }[0][4];

            if ( ! $metric_action ) {
                $metric_data = _rearrange2arrayref( $rrd_data->{ $anchor }{ $metric } );
            } 
            elsif ( $metric_action eq 'sum' ) {
                $metric_data = _calculate_sum( $rrd_data->{ $anchor }{ $metric } );
            }
            elsif ( $metric_action eq 'max' ) {
                $metric_data = _calculate_max( $rrd_data->{ $anchor }{ $metric } );
            } 
            elsif (  $metric_action eq 'min' ) {
                $metric_data = _calculate_min( $rrd_data->{ $anchor }{ $metric } );
            } 
            elsif (  $metric_action eq 'avg' ) {
                $metric_data = _calculate_avg( $rrd_data->{ $anchor }{ $metric } );
            } 

            $processed_data->{ $anchor }{ $metric } = $metric_data;
            
        }
    }

    return $processed_data;
}

=head3 _rearrange2arrayref

rearranges the data for easy later processing

IN:    rrd data organized by anchor (datacentre/midtower/logical tower) and metric
OUT:    rrd data in an arrayref of arrayrefs:  [[ (timepoint1, value1) ], [ (timepoint2, value2) ], ... ]

=cut

sub _rearrange2arrayref {
    my ( $rrd_data ) = @_;

    my $final_data = [];

    foreach my $timepoint ( sort keys %$rrd_data ) {
        push @$final_data, [ ($timepoint, $rrd_data->{ $timepoint }[0]) ];
    }

    return $final_data;
}

=head3 _calculate_sum

adds together values of all provided devices at a timepoint 

IN:    rrd data organized by anchor (datacentre/midtower/logical tower) and metric
OUT:    rrd data in an arrayref of arrayrefs:  [[ (timepoint1, value1) ], [ (timepoint2, value2) ], ... ]

=cut

sub _calculate_sum {
    my ( $rrd_data ) = @_;

    my $final_data = [];

    foreach my $timepoint ( sort keys %$rrd_data) {
        my $sum = sum @{ $rrd_data->{ $timepoint } };
        push @$final_data, [ ($timepoint, $sum) ];
    }

    return $final_data;
}

=head3 _calculate_max

calculates the max value of all provided devices at a timepoint 

IN:     rrd data organized by anchor (datacentre/midtower/logical tower) and metric
OUT:    rrd data organized by anchor (datacentre/midtower/logical tower) and metric

=cut

sub _calculate_max {
    my ( $rrd_data ) = @_;

    my $final_data = [];

    foreach my $timepoint ( sort keys %$rrd_data) {
        my $max = max @{ $rrd_data->{ $timepoint } };
        push @$final_data, [ ($timepoint, $max) ];
    }

    return $final_data;
}

=head3 _calculate_min

calculates the min value of all provided devices at a timepoint 

IN:     rrd data organized by anchor (datacentre/midtower/logical tower) and metric
OUT:    rrd data organized by anchor (datacentre/midtower/logical tower) and metric

=cut

sub _calculate_min {
    my ( $rrd_data ) = @_;

    my $final_data = [];

    foreach my $timepoint ( sort keys %$rrd_data) {
        my $min = min @{ $rrd_data->{ $timepoint } };
        push @$final_data, [ ($timepoint, $min) ];
    }

    return $final_data;
}

=head3 _calculate_avg

calculates the avg value of all provided devices at a timepoint 

IN:    rrd data organized by anchor (datacentre/midtower/logical tower) and metric
OUT:    rrd data organized by anchor (datacentre/midtower/logical tower) and metric

=cut

sub _calculate_avg {
    my ( $rrd_data ) = @_;

    my $final_data = [];

    my $count = scalar keys %$rrd_data;
    foreach my $timepoint ( sort keys %$rrd_data) {
        my $avg =  (sum @{ $rrd_data->{ $timepoint } })/$count;
        push @$final_data, [ ($timepoint, $avg) ];
    }

    return $final_data;
}

=head3 _store_metrics

stores rrd data for 5 min intervals (resolution), 

IN:    $capstat_schema
    $asset_type ... 3/4/5
    $rrd_data .......... hashref of arrayrefs containing values:  
            $dc_csw_rrd->{ $datacentre }{ $metric }   = [ [ timepoint1, value1], [timepoint2, value2], ...  ] 
                                            for $asset_type = 3
            $dc_csw_rrd->{ $midtower }{ $metric }     = [ [ timepoint1, value1], [timepoint2, value2], ...  ] 
                                            for $asset_type = 4
            $dc_csw_rrd->{ $logicaltower }{ $metric } = [ [ timepoint1, value1], [timepoint2, value2], ...  ] 
                                            for $asset_type = 5
    
    territory id (optional, used for logging logical tower metrics)

=cut

sub _store_metrics ($$$;$) {
    my ($capstat_schema, $asset_type, $rrd_data,
        $terrid) = @_;

    ## store the data in the database
    my ( $anchor_field, $metrics_table) = table_info4asset_type( $asset_type, $terrid );
    my $db = {
            asset_type    => $asset_type,
            anchor_field  => $anchor_field,
            metrics_table => $metrics_table,
         };

    ## Store in daily table
    ##    store all values for the last $delta time

    _store_timepoint_metrics( $capstat_schema, $db, $rrd_data); 
}

=head3 cleanup_old_data

IN:    type ... timepoint/weekly/monthly
    assetdb schema
    DateTime object
    daily/weekly/monthly tables

=cut
 
sub cleanup_old_data {

    my ( $type, $capstat_schema,  $dt, $metrics_table ) = @_;

    my $dt_past = $dt->clone();

    ## cleanup daily data older than 6 weeks
    if ( $type eq 'timepoint' ) {
        my ($year, $month, $week);
        my $dt_limit = $dt_past->subtract( weeks => 6 );

        ( $year, $week ) = $dt_limit->week;
        my $records       = $capstat_schema->resultset($metrics_table->{ timepoint })
                                           ->search({ '-and' => [ week  => { '<'  => $week  },
                                                                  year  => $year, ]
                                         })->delete;
    }
    ## cleanup daily data older than 3 months (15 weeks)
    elsif ( $type eq 'daily' ) {
        my ($year, $month, $week);

        my $dt_limit = $dt_past->subtract( weeks => 15 );

        ( $year, $week ) = $dt_limit->week;
        $month = $dt_limit->month;

        my $records  = $capstat_schema->resultset($metrics_table->{ daily })
                                      ->search({ '-and' => [ week  => { '<'  => $week },
                                                             month => ($month-1), ] 
                                    })->delete;
    }
    ## cleanup weekly data older than 18 months
    elsif ( $type eq 'weekly' ) {
        my ($year, $month);
        my $dt_limit = $dt_past->subtract( months => 18 );

        $month = $dt_limit->month;
        $year  = $dt_limit->year;

        my $records  = $capstat_schema->resultset($metrics_table->{ weekly })
                                      ->search({ '-and' => [ month => { '<' => $month },
                                                             year  => $year ] 
                                    })->delete;
    }
}

=head3 _store_timepoint_metrics

store values for all metrics for the last delta interval

columns in the daily tables:
    id $anchor (datacentre/midtower/logtower)  metric month week timepoint value timestamp

IN:    $capstat_schema
    $dt ............. hashref { day  => ..., 
                    month => ..., 
                    week  => ... }
    $db ............. hashref { asset_type    => ..., 
                    anchor_field  => ..., 
                    metrics_table => ... }
    $metrics_table .. hashref { daily => ..., 
                    weekly => ..., 
                    monthly => ... }
    $rrd_data ....... data hashref arrayrefs

=cut

sub _store_timepoint_metrics {
    my ($db_access,  
        $db, $rrd_data) = @_;

    my $timepoint_table = $db->{ metrics_table }{ timepoint };
    my $anchor_field    = $db->{ anchor_field };

    foreach my $anchor_id ( keys %$rrd_data ) {
        foreach my $metric_id ( keys %{$rrd_data->{ $anchor_id }} ) {
            foreach my $timevalue ( @{$rrd_data->{ $anchor_id }{ $metric_id }} ) {
                _find_or_create_metric_entry( $db_access, $timepoint_table, 
                                              $anchor_field, $anchor_id,
                                              $metric_id, $timevalue );
            }
        }
    }
    
}

=head3 store_stats

store values for all metrics for the last delta interval

IN:    
    period type ... week/month
    $capstat_schema
    $dt ............. DateTime object
    $db ............. table information
    $rrd_data ....... data hashref with arrayref values

=cut

sub store_stats ($$$$;$) {
    my ($period_type, $capstat_schema,  
        $dt,         $db, 
        $is_current) = @_;

    return unless $period_type eq 'day' or $period_type eq 'week' or $period_type eq 'month';

    my ( $table, $period, $period_info );

    my $table_period  = ( $period_type eq 'day' ) ? 'dai' : $period_type;
    $table            = $db->{ metrics_table }{ "${table_period}ly" };
    my $anchor_field  = $db->{ anchor_field };
    my $metrics_table = $db->{ metrics_table };


    $period_info = _get_period_info( $period_type, $dt, $is_current );

    ## calculate peak and 95th percentile
    ##    period_stats ... hashref
    ## ----------------------------------
    my $rrd_data     = _get_metrics4period(   $capstat_schema, $period_type,  $metrics_table,
                                              $db,             $period_info );

    my $rrd_values   = _create_values4period( $anchor_field,   $rrd_data );
    my $period_stats = _create_period_stats(  $period_type,    $rrd_values );

    ## now store the stats
    ## -------------------
    foreach my $anchor_id ( keys %$period_stats ) {
        my $anchor_stats = $period_stats->{ $anchor_id };

        foreach my $metric_id ( keys %$anchor_stats ) {
            my ( $sql_info, $key_constr ) = _create_sql4storing(
                               $anchor_field, $anchor_id,
                               $metric_id,    $period_type,
                               $period_info,  $anchor_stats->{ $metric_id });
            
            if ( $is_current ) {
                $capstat_schema->resultset( $table )
                           ->update_or_create(  $sql_info, { key => $key_constr } );

            }
            else {
                $capstat_schema->resultset( $table )
                           ->find_or_create(    $sql_info, { key => $key_constr } );
            }
        }
    }

}

=head3 get_coreswitches

finds all core switches for a given territory or all of them

IN:    schema object or Catalyst object
    which territory property to search by id/name (eu/ap etc)
    value of the property    (if not defined or -1 => will find ALL towers)

OUT:    arrayref of core switches (DBIC objects)

=cut

sub get_coreswitches {
    my ( $schema, $field, $value ) = @_;

    my ( $table_access, @core_switches );

    if ( defined $field and defined $value ) {
        return [] unless $field =~ /\A(?:id|name)\z/;

        $field = ( $field eq 'id'  ) ? 'id' : 'territory';
    }

    my $search = 
          { 
            '-or' => [
                    '-and'  => [ name => {   like      => "%xxx-%-core%" },
                                 name => { '-not_like' => '%:%' },
                                 name => { '-not_like' => '%old%' },
                                 clFK   =>   3, 
                               ],
                    '-and'  => [ name => {   like      => "yyy-%-core%" },
                                 name => { '-not_like' => '%:%' },
                                 name => { '-not_like' => '%old%' },
                                 clFK   =>   18, 
                               ],
                     ],
          };
    $search->{ 'dc.id' } = { '>'       => 0 };
    $search->{ 'dc.name' } = { '-not_in' => [ qw(AAA BBB CCC TEST) ], };
    $search->{ "territory.$field" } = $value if defined $field and
                                      $value !~ /\A-1\z/       and 
                                      $value !~ /\A0\z/;

    $table_access  = $schema->resultset('Asset');
    @core_switches = $table_access->search( 
                  $search,
                  { 'join' => { 'bay' => 
                            { 'aisle' => 
                                { 'room' => 
                                    { 'floor' => 
                                        { 'dc'   => 'territory' }}}}},
                    '+select'  => [    'territory.id', 'territory.territory', 'territory.name', 'dc.id', 'dc.name'],
                    '+as'      => [ qw( terrid            terrterritory          terrname          dcid       dcname) ], 
                    'order_by' => [ 'dcid' ],
                  } )->all;

    return \@core_switches;

}

=head3 get_firewalls

finds alther all firewalls or firewalls for a given datacentre

IN:    Catalyst object
    which firewalls to find: all or 001 etc:    all ... 001, 002 etc
    which datacentre property to search by: id/name
    value of the property    (if not defined or -1 => will find ALL towers)

OUT:    array of firewall assets (DBIC objects)

=cut

sub get_firewalls ($;$$$) {
    my ( $schema, $which, 
             $field, $value ) = @_;

    if ( defined $which ) {
        $which = ( $which =~ /\Aall\z/ ) ? '%' : "$which";
    }
    else {
        $which = '%';
    }

    my ($search);
    if ( defined $field and defined $value ) {
        return [] unless $field =~ /\A(?:id|name)\z/;

        $field = ( $field eq 'id'  ) ? 'id' : 'name';
        $search->{ "dc.$field" } = $value if 
                             $value !~ /\A-1\z/    and 
                             $value !~ /\A0\z/;
    }

    $search->{ '-and' } =  [ 'name' => {   like      => "%aaa-$which-t%" },
                             'name' => { '-not_like' => '%:%' },
                             'name' => { '-not_like' => '%old%' },
                             'parent_id' => { '!=' =>  -1 } ,
                             'parent_id' => { '!=' =>   0 } ,
                             'parent_id' => { '!=' =>   undef } ,
                             'parent_id' => { '!=' =>  '' } ,
                              ];
    $search->{ clFK }   =  4; 

    $search->{ 'dc.id' } = { '>'       => 0 };
    $search->{ 'dc.name' } = { '-not_in' => [ qw(AAA BBB CCC TEST) ], };

    my ( @firewalls );

    my $table_access = $schema->resultset('Asset');

    @firewalls = $table_access->search( 
                  $search,
                  { 'join' => 
                        { 'bay' => 
                            { 'aisle' => 
                                { 'room' => 
                                    { 'floor' => 'dc' }}}},
                    '+select'  => [ 'dc.id', 'dc.name' ],
                    '+as'      => [ qw( dcid    dcname ) ], 
                    'order_by' => [ 'dcid', 'parent_id' ],
                  } )->all;

    return \@firewalls;

}

=head3 get_servers

gets servers in a territory, datacentre or all servers if no $field and $value were supplied

IN:    Catalyst context object or schema object
    
    optional:
        search_by: dc or territory (DBIC asset relationships)
        field ... id/name : optional. If present then servers for the given datacentre will be found

OUT:    arrayref of servers as DBIC objects

=cut

sub get_servers ($;$$$) {
    my ( $schema, $anchor, $field, $value ) = @_;

    if ( defined $field and defined $value ) {
        return [] unless $field =~ /\A(?:id|name)\z/;

        if ( $anchor eq 'dc' ) {
            $field = ( $field eq 'id'  ) ? 'id' : 'name';
        }
        else {
            $field = ( $field eq 'id'  ) ? 'id' : 'territory';
        }
    }

    my $search = 
          { 
            '-and' =>  [ 'name' => {   like      => "%sss-%-t%" },
                         'name' => { '-not_like' => '%:%' },
                         'name' => { '-not_like' => '%old%' },
                         'parent_id' => { '!=' =>  -1 } ,
                         'parent_id' => { '!=' =>   0 } ,
                         'parent_id' => { '!=' =>   undef } ,
                         'parent_id' => { '!=' =>  '' } ,
                        ],
             clFK  =>   5, 
          };
    $search->{ 'dc.id' } = { '>'       => 0 };
    $search->{ 'dc.name' } = { '-not_in' => [ qw(AAA BBB CCC TEST) ], };

    $search->{ "$anchor.$field" } = $value if defined $field and
                                     $value !~ /\A-1\z/      and 
                                     $value !~ /\A0\z/;

    my ( $table_access, @servers );
    $table_access = $schema->resultset('Asset');

    @servers = $table_access->search( 
                  $search,
                  { 'join' => 
                        { 'bay' => 
                            { 'aisle' => 
                                { 'room' => 
                                    { 'floor' => 
                                        { 'dc' => 'territory' } }}}},
                    '+select'  => [    'territory.id', 'territory.territory', 'territory.name', 'dc.id', 'dc.name'],
                    '+as'      => [ qw( terrid          terrterritory          terrname          dcid     dcname) ],
                    'order_by' => [    'dcid', 'parent_id' ],
                  } )->all;

    return \@servers;
}

#=========================================== PRIVATE METHODS ===========================================
=head3 _get_period_info

finds the correct year/month/week depending on whether we want to store stats for the current or
previous month/week/day

IN:     period type: day/week/month
        DateTime object
        is_current ... optional, if given => we are storing for this week/month

OUT:    hashref with year/month/week keys

=cut

sub _get_period_info {
    my ($period_type, $dt_orig, $is_current ) = @_; 

    my ( $year, $week, $month, $day, $previous_period );
    my $dt = $dt_orig->clone();

    ## we shall be storing stats for the previous:
    ## ------------------------------------------
    if ( not $is_current ) {

        ## DAY
        if ( $period_type eq 'day' ) {
            $previous_period = $dt->subtract( days => 1 );
        }
        ## WEEK
        elsif ( $period_type eq 'week' ) {
            $previous_period = $dt->subtract( weeks => 1 );
        }
        ## MONTH
        elsif ( $period_type eq 'month' ) {
            $previous_period = $dt->subtract( months => 1 );
        }
        ## YEAR
        else {
            $previous_period = $dt->subtract( years => 1 );
        }

        ( $year, $week ) = $previous_period->week;
          $month         = $previous_period->month;
          $day           = $previous_period->day_of_year;
    } 

    ## CURRENT
    else {
        ( $year, $week, $month, $day ) = ( $dt->week, $dt->month, $dt->day_of_year );
    }

    my $period_info = {
                        year  => $year,
                        month => $month,
                        week  => $week,
                        day   => $day,
                      };
    return $period_info;
}

=head3 _get_metrics4period

returns a DBIC resultset with metric data for the previous day/week/month 

=cut

sub _get_metrics4period ($$$$$) {
    my  ( $capstat_schema, $period_type,  $metrics_table, 
          $db,             $this_period ) = @_;

    my $search;
    if ( $period_type eq 'day' ) {
        $search = {
                day    => $this_period->{ day },
                year   => $this_period->{ year },
              };
    }
    elsif ( $period_type eq 'week' ) {
        $search = {
                week   => $this_period->{ week },
                year   => $this_period->{ year },
              };
    }
    elsif ( $period_type eq 'month' ) {
        $search = {
                month  => $this_period->{ month },
                year   => $this_period->{ year },
              };
    }
    else {
        return;
    }

    my $data = $capstat_schema->resultset($metrics_table->{ timepoint })
                   ->search(   $search, 
                            { 'order_by'   => { '-desc' => 'value' } });

}

=head3 _create_values4period

=cut

sub _create_values4period {
    my  ( $anchor_field, $rrd_data ) = @_;

    my $rrd_values;
    while ( my $row = $rrd_data->next ) {
        push @{ $rrd_values->{$row->$anchor_field}{$row->metric} }, $row->value;

    }

    return $rrd_values;

}

=head3 _create_period_stats

=cut

sub _create_period_stats ($$) {
    my  ( $period_type, $rrd_values ) = @_;

    my ( $stats );
    foreach my $anchor ( keys %$rrd_values ) {

        my $anchor_values = $rrd_values->{ $anchor };
        foreach my $metric ( keys %$anchor_values ) {

            my $metric_values = $anchor_values->{ $metric };

            $stats->{ $anchor }{ $metric }{ value_peak } = $metric_values->[0] || 0;
            $stats->{ $anchor }{ $metric }{ value_95th } = _get_95th($metric_values);
        }
    }

    return $stats;

}

=head3 _create_sql4storing

creates hashref for creating or updating a stats record

=cut

sub _create_sql4storing {
    my ($anchor_field, $anchor_id,
        $metric_id,    $period_type,
        $period_info,  $anchor_metric_stats) = @_;

    my $sql_info =     
        {
            $anchor_field => $anchor_id,
            metric        => $metric_id,
        };
    my $key_constr;

    if ($period_type eq 'day' ) {
        $sql_info->{ month } = $period_info->{ month };
        $sql_info->{ week }  = $period_info->{ week };
        $sql_info->{ day }   = $period_info->{ day };
        $key_constr = 'uniq_daily';
    }
    if ($period_type eq 'week' ) {
        $sql_info->{ year }  = $period_info->{ year };
        $sql_info->{ month } = $period_info->{ month };
        $sql_info->{ week }  = $period_info->{ week };
        $key_constr = 'uniq_weekly';
    }
    if ($period_type eq 'month' ) {
        $sql_info->{ year }  = $period_info->{ year };
        $sql_info->{ month } = $period_info->{ month };
        $key_constr = 'uniq_monthly';
    }

    $sql_info->{ value_peak } = $anchor_metric_stats->{ value_peak };
    $sql_info->{ value_95th } = $anchor_metric_stats->{ value_95th }; 

    return ( $sql_info, $key_constr );
}

=head3 _normalize_device_name

strips csw-001-core.cop1.dk.scansafe.net to csw-001-core.cop1

=cut

sub _normalize_device_name {
    my ($name) = @_;

    $name =~ s/\A\s*([a-zA-Z0-9\-]+)\.([a-zA-Z0-9]+)\..*\z/$1.$2/;
    return $name;
}


1;
