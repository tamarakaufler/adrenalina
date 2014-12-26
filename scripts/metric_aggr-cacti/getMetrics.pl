#!/usr/bin/perl
use strict;
use warnings;

# =====================================================================================================
#                 getMetricsStatusInfo.pl
#                ------------------------
# script for collecting Metrics Status stats from Cacti.
# it runs daily as a cron job to access Cacti rrd files and capture required
# information, process it and store in Asset database
# =====================================================================================================

use Benchmark;

use FindBin qw($Bin);
use lib "$Bin";

use POSIX ":sys_wait_h";
use CHI;

use Helper::MetricsStatus  qw(  
                get_config
                get_asset_schema
                get_schemas
                get_rrd_rules
                get_rrd_path
                get_cacti_data
                afw4metrics_status
                csw4metrics_status
                logtw4metrics_status
                process_csw_metrics
                process_afw_metrics
                process_logtw_metrics

                get_coreswitches
                get_firewalls
                get_servers
                       );

# we need some config info for sending email and connecting to databases
#use CMDBConfig qw(returnConfig);

my $config_file    = "$Bin/config.yml";
my $config         = get_config( $config_file );

my $cacti_config   = $config->{ cacti };
my $capstat_config = $config->{ metrics_status };
my $asset_config    = $config->{ asset };

my $rrd_dir        = $config->{ rrd }{ dir };
my $rrd_cache_file = $config->{ rrd }{ cache_file };
my $rrd_cache_time = $config->{ rrd }{ cache_time };

my $rrd_cache = CHI->new( driver        => 'File',
              		  expires_in    => $rrd_cache_time,
                  	  root_dir      => "/tmp/$rrd_cache_file",
);

#================================================================================================
#                        MAIN SCRIPT
#================================================================================================

my $db         = get_schemas(     $capstat_config, $cacti_config );
my $asset_schema = get_asset_schema( $asset_config );

my $rrd_info =     {
            dir     	=> $rrd_dir,
            cache_time 	=> $rrd_cache_time,
            cache    	=> $rrd_cache,
        };
## ----------------------------------------------------------------------------------------------
## get CORE SWITCH devices
## -----------------------
my ($t0, $t1, $t2, $t3, $td);
print scalar localtime(time) . "\n";
$t0 = Benchmark->new;
$t1 = $t0;

    #my $core_switches = get_coreswitches($asset_schema, 'name', 'eu');
    my $core_switches = get_coreswitches($asset_schema);
    print " number of core switches: " . scalar @$core_switches . "\n\n";

$t2 = Benchmark->new;
$td = timediff($t2, $t1);
print "*** get_coreswitches took:",timestr($td),"\n";
$t1 = Benchmark->new;

    my $csw_ref       = csw4metrics_status($core_switches);

$t2 = Benchmark->new;
$td = timediff($t2, $t1);
print "*** csw4metrics_status took:",timestr($td),"\n";

## ----------------------------------------------------------------------------------------------

## get FIREWALL devices
## --------------------
$t1 = Benchmark->new;

    #my $firewalls = get_firewalls($asset_schema, '001','id', 18);
    my $firewalls = get_firewalls($asset_schema, '001');
    print " number of firewalls: " . scalar @$firewalls . "\n\n";

$t2 = Benchmark->new;
$td = timediff($t2, $t1);
print "*** get_firewalls took:",timestr($td),"\n";
$t1 = Benchmark->new;

    my $afw_ref   = afw4metrics_status($firewalls);

$t2 = Benchmark->new;
$td = timediff($t2, $t1);
print "*** afw4metrics_status took:",timestr($td),"\n";

## ----------------------------------------------------------------------------------------------
## ----------------- Set up database connection and rules for finding RRD files -----------------

    ## Get RRD rules for finding the relevant RRD files (rules per device type and stats type)
    my $rule = get_rrd_rules( $db->{ metrics_status } );

## RRD time factors for fetching rrd data
##---------------------------------------
my ( $delta, $latency );

## latency makes sure the last data of the delta interval have actually been already collected
$latency = 3600; 
my $end_time = time - $latency;        # now - 1 hour

## delta == desired period of time + 0.5h for overlap

## 48600 == 12.5h
$delta = 45000;

my $start_time = $end_time - $delta;

print "\n\n*** START TIME :($start_time)" . scalar localtime($end_time - $delta) . "\n";
print     "*** END TIME   :($end_time)"   . scalar localtime($end_time) . "\n\n";

my $rrd_time = { end_time => $end_time, delta => $delta };

## ----------------------------------------------------------------------------------------------

$t1 = Benchmark->new;
my $csw_rule = $rule->{ 3 };

    process_csw_metrics($rrd_info, $db, { rule => $csw_rule, time => $rrd_time  }, $csw_ref);

$t2 = Benchmark->new;
$td = timediff($t2, $t1);
print "*** process_csw_metrics:",timestr($td),"\n";

##++++++++++++++++++++++++++++++

$t1 = Benchmark->new;
my $afw_rule = $rule->{ 4 };

    process_afw_metrics($rrd_info, $db, { rule => $afw_rule, time => $rrd_time  }, $afw_ref);

$t2 = Benchmark->new;
$td = timediff($t2, $t1);
print "*** process_afw_metrics:",timestr($td),"\n";

$t1 = Benchmark->new;
my $logtw_rule = $rule->{ 5 };
#-------------------------------------------------------------------------------------------------

## get LOGICAL TOWERS devices
## --------------------------

## SHALL WE PARALLELIZE ON DATACENTRE LEVEL (currently set up only for region 5 : 
#                        Helper/MetricsStatus.pl :
#                       _process_datacentre_logtowers subroutine)
my $parallelize = 0;

my %child = ();

## get the territories
## -------------------
my @territories = $asset_schema->resultset('Territory')->search({}, { 'select' => [ 'id' ] })->all;
@territories = map { $_->id } @territories;

foreach my $terr ( @territories ) {

    print "*** START process_logtw_metrics for territory $terr ... ", scalar localtime(time),"\n";

    my $pid = fork();
    
    if ( not defined $pid ) {
        print "ERROR: could not collect and process capacity status stats for territory $terr\n" 
                                    and next;
    }
    elsif ( $pid ) {
        $child{ $pid } = $terr;
    }
    else {

        $t1 = Benchmark->new;
        
            my $servers = get_servers($asset_schema, 'territory', 'id', $terr);
            print "*** in forked child for $terr: number of servers: " . scalar @$servers . "\n\n";
        
        $t2 = Benchmark->new;
        $td = timediff($t2, $t1);
        print "\t\t*** in forked child for  $terr: get_servers took:",timestr($td),"\n";
        $t1 = Benchmark->new;
        
            my $logtw_ref       = logtw4metrics_status($servers);
        
        $t2 = Benchmark->new;
        $td = timediff($t2, $t1);
        print "*** in forked child for $terr: logtw4metrics_status took:",timestr($td),"\n";
        #--------------------------------------------------------------------------------------------
            print "*** in forked child for $terr: START process_logtw_metrics: ", scalar localtime(time),"\n";
            process_logtw_metrics(  $rrd_info, $db, 
                           { rule => $logtw_rule, time => $rrd_time  }, 
                        $logtw_ref, $parallelize);
            print "*** in forked child for $terr: END process_logtw_metrics: ", scalar localtime(time),"\n";
        
        $t2 = Benchmark->new;
        $td = timediff($t2, $t1);
        print "*** disattaching child for territory $terr: ",timestr($td),"\n";

        exit(0);

    }

    print "*** END of processing territory $terr ... ", scalar localtime(time),"\n";

}

## reap dead children
while ( keys %child ) {
    foreach my $pid ( keys %child ) {
        my $result = waitpid $pid, WNOHANG;
        delete $child{ $pid } and print "\t>>> Discarded $pid\n" if $result == $pid;
    }
}

$t2 = Benchmark->new;
$td = timediff($t2, $t0);
print scalar localtime(time) . "\n";
print "*** TOTAL TIME - script took: ",timestr($td),"\n";

