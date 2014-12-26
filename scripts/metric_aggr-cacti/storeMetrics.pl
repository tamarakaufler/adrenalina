#!/usr/bin/perl
use strict;
use warnings;

# =====================================================================================================
#                storeMetricsStats.pl
# script for calculating Metrics Status stats
# it runs daily as a cron job to process metrics stored in the database and calculate daily/weekly/monthly stats
# =====================================================================================================

use Benchmark;

use FindBin qw($Bin);
use lib $Bin;

use Helper::MetricsStatus qw(  
                                get_config
                                table_info4asset_type
                                get_date_info
                                get_capstat_schema
                                store_stats
                                cleanup_old_data
                            );

# we need some config info for sending email and connecting to databases

my $config_file = "$Bin/config.yml";
my $config = get_config( $config_file );

#================================================================================================
my ($t0, $t1, $t2, $t3, $td);
print scalar localtime(time) . "\n";
$t0 = Benchmark->new;

my ( $capstat_schema, $dt );
$capstat_schema = get_capstat_schema( $config->{ metrics_status } );

## returns date_href and DateTime object (now)
( undef, $dt )  = get_date_info();

## fake date:
#=  $dt = DateTime->new(
#=      year       => 2012,
#=      month      => 10,
#=      day        => 1,
#=      hour       => 5,
#=      minute     => 30,
#=      second     => 0,
#=  );

## Perform stats calculations for core switches, firewalls and logical towers
## ==========================================================================
foreach my $asset_type ( 3 .. 5 ) {

    my $t1_asset = Benchmark->new;

    ## due to parallelization of gathering stats from the rrd files by regions and storing them in
    ## separate tables, we need to process each table separately
    if ( $asset_type == 5 ) {
        foreach my $terrid ( 4 .. 9 ) {
            _process_data( $dt, $asset_type, $terrid );
        }
    }
    else {
        _process_data( $dt, $asset_type );
    }

    $t3 = Benchmark->new;
    $td = timediff($t3, $t1_asset);
    print "\n*** storing stats for $asset_type took: ",timestr($td),"\n\n";

}

$t3 = Benchmark->new;
$td = timediff($t3, $t0);
print "\n*** TOTAL TIME: storing stats for all asset types took: ",timestr($td),"\n";

=head3 _process_data

=cut

sub _process_data {
    my ( $dt, $asset_type, $terrid ) = @_;

    my ( $t1, $t2, $td );
    my ($anchor_field, $metrics_table) = table_info4asset_type( $asset_type, $terrid );
    my $db_info = {
                asset_type    => $asset_type,
                anchor_field  => $anchor_field,
                metrics_table => $metrics_table,
               };

    ## Store in daily table
    ## --------------------
    ##    store all values for the previous day

        $t1 = Benchmark->new;
    
        store_stats( 'day', $capstat_schema, $dt, $db_info);

        $t2 = Benchmark->new;
        $td = timediff($t2, $t1);
        print "*** storing DAILY stats for $asset_type took: ",timestr($td),"\n";
    
    ## store in weekly table if Monday and it's the first run of the day
    ##    find all values for the given anchor and metric between 
    ##    last Mon 00:00:05 and today 00:00:00

    ## calculate week peak/95th percentile from daily values
    ## -----------------------------------------------------

    if ( $dt->day_of_week == 1 ) {

        $t1 = Benchmark->new;
    
        ## let's cleanup the old data first
        cleanup_old_data( 'timepoint', $capstat_schema, $dt, $metrics_table );
        store_stats(      'week',      $capstat_schema, $dt, $db_info);

        $t2 = Benchmark->new;
        $td = timediff($t2, $t1);
        print "*** storing WEEKLY stats for $asset_type took: ",timestr($td),"\n";
    }

    ## store in monthly table if the first day of the month and the first run of the day
    ##    find all values for the given anchor and metric between 
    ##    first day of the last month 00:00:05 and last day of last month 00:00:00
    
    if ( $dt->day == 1 ) {

        $t1 = Benchmark->new;
    
        ## let's cleanup the old data first
        cleanup_old_data( 'daily',  $capstat_schema, $dt, $metrics_table );
        cleanup_old_data( 'weekly', $capstat_schema, $dt, $metrics_table );
        store_stats(      'month',  $capstat_schema, $dt, $db_info);

        $t2 = Benchmark->new;
        $td = timediff($t2, $t1);
        print "*** storing MONTHLY stats for $asset_type took: ",timestr($td),"\n";
    }

}

exit;

