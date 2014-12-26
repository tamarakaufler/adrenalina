#!/usr/bin/perl

use strict;
use warnings;

# =====================================================================================================
#                    dailyXXXreport.pl
#                ------------------------
# script for getting bi-daily information about servers out of the load balancer
# it runs twice a day as a cron job
#
# CAVEAT
#
# The current logic is based on the assumption the scripts runs twice a day, morning and evening.
# If a server found out of service in the moprning, is back in service (load balancer) in the afternoon),
# then its morning records is deleted.
# If the script is required to run only once a day, or once several days, the _handleISserver can be 
# easily changed to accommodate this:
#
#    if ( $morning ) {
#        return unless exists $servers2report_daily->{ "${ip}am" };
#        print "\t\t\tdeleting am servers2report_daily\n";
#        delete $servers2report_daily->{ "${ip}am" };
#    }
#    else {
#        return unless exists $servers2report_daily->{ "${ip}pm" };
#        print "\t\t\tdeleting pm servers2report_daily\n";
#        delete $servers2report_daily->{ "${ip}pm" };
#    }
#            
# # =====================================================================================================

use Benchmark;
use DB_File;
use Fcntl;
use FindBin qw($Bin);

use lib "/opt/Cmdb/lib";
use lib "$Bin";

use ASSETConfig qw(returnConfig);

use DateTime;
use POSIX qw( tzset );

use Helper::OOSreports     qw(  
                get_config
                get_asset_schema
                get_asset_dbh
                getLoadBalancers
                getInServiceState4MidTower
                getLiveLogicalTowers
                attachServers
                recordLogTowerOOSServers
                sendEmail
                    );

{


local $ENV{'TZ'} = 'Europe/London';
tzset();

# we need some config info for sending email and connecting to databases
#use ASSETConfig qw(returnConfig);

my $config_file  = "$Bin/config.yml";
my $config       = get_config( $config_file );

my $ASSETConfig  = returnConfig();

## this depends on the ASSET config file
my $asset_config = $ASSETConfig->{ ASSET };

#================================================================================================
#                        MAIN SCRIPT
#================================================================================================

my $schema = get_asset_schema( $asset_config );
my $dbh    = get_asset_dbh(    $asset_config );

my $email_info;
$email_info->{ from }    = $config->{email}->{ from },
$email_info->{ to }      = $config->{email}->{ to },
$email_info->{ subject } = $config->{email}->{ subject },
$email_info->{ smtp }    = $config->{email}->{ smtp },

## query to find servers for towers that are LIVE
#================================================
#select ciName, ciID, parentCIID  from ci_object cio where (cio.clFK=5 or cio.clFK=11) and ciName not like '%old%' and ciName like ...................  and cio.parentCIID != -1 and cio.parentCIID != 0 and cio.parentCIID is not NULL and cio.parentCIID not in (select ciID from ci_object left join attributes on attributes.amCIID =ci_object.ciID left join attribute_map .... left join attribute_lookup on attribute_map.alFK = attribute_lookup.atID left join attribute_options .... and attribute_lookup.subattr ='NotYetLive'  group by ciID);

## ----------------------------------------------------------------------------------------------
my $date = DateTime->now( time_zone => 'Europe/London' );
my $dow  = $date->dow;
my $hour = $date->hour;

$date->subtract( weeks => 1 );
my ( $start_epoch, $epoch ) = ( $date->epoch(), time );

my ( $start, $end )    = ( scalar localtime($start_epoch), scalar localtime($epoch) );
$email_info->{ start } = $start;
$email_info->{ end }   = $end;

my ($t0, $t1, $t2, $t3, $td);
print scalar localtime($epoch) . "\n";

$t0 = Benchmark->new;
$t1 = $t0;

    my $dailyOOSfile  = "$Bin/dailyOOS.bdb";
    my $weeklyOOSfile = "$Bin/weeklyOOS.bdb";

    tie my %servers2report_daily,  'DB_File', $dailyOOSfile,  O_CREAT|O_RDWR, 0666, $DB_BTREE or die $!;
    tie my %servers2report_weekly, 'DB_File', $weeklyOOSfile, O_CREAT|O_RDWR, 0666, $DB_BTREE or die $!;

    my %logtower;
    my $midtower = getLoadBalancers( $schema );

    ## get all live logical towers ( stored in BerkeleyDB a : { $logtwid => 
    #                                { name    =>..., 
    #                                  parent  => ..., 
    #                                  servers => [ { ip => ..., name => ... },
    #                                           { ip => ..., name => ... }, 
    #                                           ...] 
    #                                 },
    #                                 $logtwid => ....  })

    getLiveLogicalTowers( $dbh, $schema, \%logtower );

    my @logtw_ids = keys %logtower;

    ## get servers for all live logical towers: sets servers hash key for each logtower
    attachServers($schema, \%logtower);

    $t1 = Benchmark->new;

    my @errors = ();

    my $bdb_hash = { daily  => \%servers2report_daily, 
             weekly => \%servers2report_weekly };

    ## get midtower state ( shared by all its logical towers )
    foreach my $midtw ( keys %$midtower ) {
        $t1 = Benchmark->new;
        print scalar localtime($epoch) . ":  MidTower $midtw starting\n";
        my ($state, $work_state, $lb_type) = getInServiceState4MidTower( $midtower->{ $midtw }{lbs}, 
                                                     $config );

        print STDERR "\tERROR: no state found - problems with load balancers " . 
                                        $midtower->{ $midtw }{ name } . ' (' . 
                                        $midtower->{ $midtw }{ dc } . ")\n" && next unless $state;

        ## check which servers are and store in a BDB
        foreach my $logtwid ( @logtw_ids ) {

            ## this could be written more efficiently by having the logtower hash keyed on mistower, then logtower id
            if ( $logtower{ $logtwid }{ parent } == $midtw ) {
                ## check servers of this tower => store in daily BDB        
                recordLogTowerOOSServers($bdb_hash, $state, $work_state,
                             $midtower->{ $midtw }{ dc }, 
                             $lb_type,
                             $logtower{ $logtwid }{ name }, 
                             $logtower{ $logtwid }{ servers },
                             $hour);
            }
        }
        $t2 = Benchmark->new;
        $td = timediff($t2, $t1);

        print scalar localtime($epoch) . ":  $midtw done\n\n";
        print "*** finding servers for mid-tower $midtw took:",timestr($td),"\n\n\n";
    }

    ## send the bi-daily report
    ##-------------------------
    sendEmail($email_info, \%servers2report_daily );
    
    ## send the weekly report
    ##-----------------------
    if ( $dow == 5 && $hour < 12 ) {
        $email_info->{ subject } .= ' - Weekly Summary' ;
        sendEmail($email_info, \%servers2report_weekly, 1 );
        unlink $weeklyOOSfile;
    }

    untie %servers2report_daily;
    untie %servers2report_weekly;

$t2 = Benchmark->new;
$td = timediff($t2, $t0);
print "*** finding servers took:",timestr($td),"\n";
$t1 = Benchmark->new;

}

exit;

