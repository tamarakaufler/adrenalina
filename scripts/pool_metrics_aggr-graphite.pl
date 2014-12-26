#!/usr/bin/perl

=head2 pool_metrics_aggr

script for aggregating graphite metrics of VMs in a pool and storing them in graphite Whisper files

can run in either of two modes:
				collects metrics once for selected time interval
				runs as a daemon, collecting data in predetermined minute intervals
=cut

use strict;
use warnings;
use v5.010;

use JSON qw(encode_json decode_json);
use Getopt::Long;
use LWP::UserAgent;

use IO::Socket::INET;

use Fcntl qw(:flock);
use Proc::Daemon;
use CHI;
use YAML::Tiny;
use Getopt::Long;

use DBI;
use DateTime;
use Data::Dumper qw(Dumper);

use File::stat;
use Time::localtime;

use threads;
use Parallel::ForkManager;

## Mutex - prevent multiple instances of this script running simultaneously
#-------

BEGIN {
        open  *{0}            or die("$0 is already running ...");
        flock *{0}, LOCK_EX|LOCK_NB or die "Cannot lock - $!";
}

## Process command line arguments
#-------------------------------

my $is_daemon   = 0;
my $help	= 0;

my $delta_type;
my $from;
my $until;

GetOptions(
		"daemon|d+"  => \$is_daemon,
		"from|f=s"   => \$from,
		"until|u=s"  => \$until,
		"help|h+"    => \$help,	
	  );

if ($help) {
	say <<HELP;
Usage: 	
	script for aggregating pool aggregates and storing them in graphite

	perl pool_metrics_aggr.pl --daemon
	perl pool_metrics_aggr.pl -d
	perl pool_metrics_aggr.pl --from=6000min --until=3000min
	perl pool_metrics_aggr.pl --from=5hour --until=30min

	type options: min/hour/day

--daemon ... to start the script as daemon
-d       ... to start the script as daemon

--from   ... how many minutes/hours/days ago should the date collection start
--until  ... how many minutes/hours/days ago should the date collection end

HELP
exit;
}

## Caching
#---------

# caching of  CWS loadbalancer IPs: cws_lbs => [ $ip1 $ip2 etc ]
# caching for pool VM hosts: { pool_vm => { $ip1 => $hostname1 etc } }

my $cache = CHI->new( 	driver 	 => 'File',
        		root_dir => '/tmp/pool_metrics_aggr'
		    );

#==============================================================================================================
# Configuration
# -------------

my $config_file = 'capstat_scripts.yml';

my $yaml   = YAML::Tiny->read( $config_file ) or die "ERROR: cannot access $config_file: $!";
my $config = $yaml->[0];

#print Dumper($config); exit;

my ($period_info, $metrics_info, $graphite_connect, $graphite_carbon); 	

$period_info       = $config->{ period };
$period_info->{FROM}  = $from  if $from;
$period_info->{UNTIL} = $until if $until;

my $cms_connect       = $config->{ cms }{ db };
my $cmsdash_connect   = $config->{ cmsdash }{ db };
$graphite_connect     = $config->{ graphite }{ connect };
$graphite_carbon      = $config->{ graphite }{ carbon };

my $alb_info         = { ident   => $config->{ alb }{ ident },
		         connect => $config->{ alb }{ connect } }; 
my $metrics_ident    = $config->{ metrics }{ csw_pool };

#==============================================================================================================
# Initial setup
# -------------

## API call to the ALB
my $ua = LWP::UserAgent->new();
$ua->timeout(20);

my $sock = IO::Socket::INET->new(
        PeerAddr => $graphite_carbon->{ host },
        PeerPort => $graphite_carbon->{ port },
        Proto    => 'tcp',
	Listen   => 20,
);
die "Unable to connect: $!\n" unless ($sock->connected);

# ------- 
#my $url = "http://graphite.xxxxxxx.company.com/render/?from=-10min&until=-0min&target=sumSeries(server1.interface.octets.mgmt.tx,server2.interface.octets.mgmt.rx)&format=json";
#my $response = get($url);
#print $response->decoded_content; 
#print Dumper($response); exit;
# ------- 

my ($cms_dbh, $cmsdash_dbh);
my $cms_host 	= $cms_connect->{ host };
my $cms_db 	= $cms_connect->{ db };
my $cms_dbd 	= $cms_connect->{ dbd };
my $cms_user 	= $cms_connect->{ user };
my $cms_pass 	= $cms_connect->{ pass };
$cms_dbh 	= DBI->connect("dbi:$cms_dbd:$cms_db:$cms_host", $cms_user, $cms_pass) 
										or die $DBI::errstr;
my $cmsdash_host = $cmsdash_connect->{ host };
my $cmsdash_db 	 = $cmsdash_connect->{ db };
my $cmsdash_dbd  = $cmsdash_connect->{ dbd };
my $cmsdash_user = $cmsdash_connect->{ user };
my $cmsdash_pass = $cmsdash_connect->{ pass };
$cmsdash_dbh 	 = DBI->connect("dbi:$cmsdash_dbd:$cmsdash_db:$cmsdash_host", $cmsdash_user, $cmsdash_pass) 
										or die $DBI::errstr;
# ========================================================================

## Get CWS loadbalancer hostnames
# (used to get information about pools contents)
#-----------------------------------------------
# CAVEAT:
# when changes are made to the CWS loadbalancer infrustructure, the change should trigger invalidation of the cache

my $cws_lbs = $cache->get('cws_lbs');
if ( ! defined $cws_lbs or ! scalar @$cws_lbs) {

	say "NO CACHE!!";
	$cws_lbs = get_cws_albs($cms_dbh, $alb_info->{ ident });

	## TODO: change expiry to a sensible value 
	$cache->set('cws_lbs', $cws_lbs, 'now');
}

$alb_info->{ hostnames } = $cws_lbs;
# ========================================================================

## Get pool info
#
---------------
## hashref { pools => [], pool_servers => {} }
my $pools_info = _get_pool_info($cms_dbh, $ua, $alb_info);

## Get metrics
#-------------

$metrics_info = get_metrics_info($cmsdash_dbh, $metrics_ident);
#==============================================================================================================
# MAIN 
# ====

my $graphite_url_template = _create_graphite_url_template($graphite_connect, $period_info);
#print 'graphite_url_template: ' . Dumper($graphite_url_template);

my $graphite_info = {
			host	     => $graphite_connect->{ host },
			url_template => $graphite_url_template,
		        prefix	     => $graphite_connect->{ prefix },
		    };

if ($is_daemon) {

	my $daemon = Proc::Daemon->new;
	$daemon->Init({});

	my $loop_delta = $config->{loop_delta};

	my ($start_epoch, $end_epoch);
	my ($start_dt);

	while (1) {
		$start_epoch      = time();

		## TODO ---------------------------------------------------
		# Check time and:
		#	get pools every day : 	uses the cached info which is refreshed 
		#				on a regular basis as required
		#	get metric info  every hour
		## TODO ---------------------------------------------------

		pool_metrics_aggr($graphite_info,
				$metrics_info, $pools_info);

		## delay next loop iteration if required
		#---------------------------------------
		$end_epoch        = time();
		my $elapsed_delta = $end_epoch-$start_epoch;

		say "loop_delta: $loop_delta - elapsed: $elapsed_delta , " . 
			' need to wait: ' . ($loop_delta - $elapsed_delta);
		say '------------------------------------------------------';

		sleep ($loop_delta - $elapsed_delta) if $elapsed_delta > 0 && $elapsed_delta < $loop_delta;
	}

}
else {
	pool_metrics_aggr($graphite_info, $metrics_info, $pools_info);
}

# create graphite query aggregating across the pool (ie pool's servers) and get the data
# --------------------------------------------------------------------------------------
## http://graphite.xxxxxxxxx.company.com/render/?width=586&height=308&_salt=1372075213.989&target=aaaa.graphite.interface.packets.eth0.rx&target=aaaa.graphite.interface.packets.eth0.tx&target=sum%28aaaa.graphite.interface.packets.eth0.tx,aaaa.graphite.interface.packets.eth0.rx%29
#
## http://graphite.zzzzzzzzz.company.com/render?width=588&height=310&_salt=1375267485.024&target=nPercentile%28common.aaaa.graphite.interface.octets.mgmt.rx%2C95%29
# ========================================================================

=head2 PUBLIC METHODS

=head3 pool_metrics_aggr

=cut

sub pool_metrics_aggr {
	my ($graphite_info,
	    $metrics_info, $pools_info,
	    ) = @_;

	my $pm= Parallel::ForkManager->new(20);
	$pm->run_on_finish( 
		sub {
	      		my ($pid, $exit_code, $ident) = @_;
	      		print "\t\t*** process $pid just got out of the pool with exit code: $exit_code\n";
	  	});

	my @dcnames = keys %$pools_info;
	foreach my $dcname ( @dcnames ) {

		$pm->start and next;
		
		my @pools = keys %{$pools_info->{$dcname}};
		foreach my $pool ( @pools ) {

			## TODO
			## Check time:
			#	get pool content every hour

			my $vms = _massage_vm_names({ prefix => $graphite_info->{prefix}, 
					    dcname => $dcname },
					    $pool,
					    $pools_info);

			my $pool_info = { id  => $pool,
					  dc  => $dcname,
					  vms => $vms };
		
			create_and_pool_metrics_aggr($graphite_info,
						   $metrics_info, $pool_info);	
		}

		$pm->finish;
	}
	$pm->wait_all_children;
}
		

=head3 create_and_pool_metrics_aggr

=cut

sub create_and_pool_metrics_aggr {
	my ($graphite_info,
	    $metrics_info, $pool_info ) = @_;

	my $graphite_url_template = $graphite_info->{ url_template };
	my @metric_types = keys %$metrics_info;

	my @pool_metric_aggr = ();
	my %children = ();

	my @threads = ();

	foreach my $metric_type (@metric_types) {
		my @metrics = keys %{$metrics_info->{ $metric_type }};
		foreach my $metric (@metrics) {
			my $metric_info = [$metric, 
					   $metrics_info->{ $metric_type }{ $metric }];
			for my $aggr ( keys %$graphite_url_template ) {
				$graphite_url_template->{ $aggr } =~ s/AGGR/$metric_info->[1]/g;

				my $thr = threads->create(\&_create_and_pool_metrics_aggr, 
							   $graphite_info,
							   $graphite_url_template->{ $aggr }, 
							   $metric_info->[0],
							   $pool_info);
				push @threads, $thr;
			}
		}
	}

	for my $thr (@threads) {
 		if ($thr->is_joinable()) {
			my $result = $thr->join();

			push @pool_metric_aggr, $result;
		}
	}
	
	return \@pool_metric_aggr;
	
}

=head3 get_metrics_info

=cut

sub get_metrics_info {
	my ($cmsdash_dbh, $metrics_ident) = @_;

	my $raw_data = get_raw_scan_metrics($cmsdash_dbh, $metrics_ident);
	my $massaged = massage_scan_metrics($raw_data);

	my $metric_info = _get_raw_metrics_path($cmsdash_dbh, $massaged);

	return $metric_info;
}

=head3 get_graphite_aggr_data

gets graphite data for a host's metric for a particular time period (in json)

=cut

sub get_graphite_aggr_data {
	my ($graphite_connect, 
	    $host, $metric_info, $period_limit ) = @_;

	my $raw_data = get_raw_aggr($graphite_connect, 
				    $host, $metric_info, $period_limit);
}

=head3 get_raw_scan_metrics

gets metrics for pool VMs

IN:	db handle for the capacity db
	metrics identifiers for pool VMs
OUT:	hashref { host    => dbi arrayref,
		  product => dbi arrayref } with metric/rank information

=cut

sub get_raw_scan_metrics {
	my ($cmsdash_dbh, $ident) = @_;
	
	my $raw_data;
	my @types         = keys %$ident;

	foreach my $metric_type ( @types ) {
		my @columns = keys %{$ident->{$metric_type}};
		my @where;
		foreach my $column ( @columns ) {
			push @where, "$column = '$ident->{$metric_type}{$column}'";
		}
		my $where = join ' AND ', @where;
		
		my $sql = "SELECT metric 
			   FROM host_metric_$metric_type 
			   WHERE $where";

		$raw_data->{ $metric_type } = $cmsdash_dbh->selectall_arrayref($sql);
	}
	
	return $raw_data;

}

=head3 massage_scan_metrics

massages dbi raw data into a suitable data structure

=cut

sub massage_scan_metrics {
	my ($data) = @_;

	my $massaged = {};
	my @types    = keys %$data;
	foreach my $metric_type ( @types ) {
		my @metrics = map { $_->[0] } @{$data->{ $metric_type }}; 
		$massaged->{ $metric_type } = \@metrics;
	}

	return $massaged;
	
}

=head3 get_raw_aggr

OUT:	[
	 {"target": 	"<server>.interface.octets.mgmt.rx", 
	  "datapoints": [[255.0, 1376297520], [244.566667, 1376297580], ... ]},
	 {"target": 	"common.lon5.fw-brd-02-lon5-admin.interface.octets.mgmt.rx", 
	  "datapoints": [[255.0, 1376297520], [244.566667, 1376297580], ... ]},
	]

=cut

sub get_raw_aggr {
	my ($graphite_connect, 
	    $hosts, $metric_info, $period_limit ) = @_;

#	my $request = 
}

=head3 get_cws_albs

=cut

sub get_cws_albs {
	my ($cms_dbh, $alb_ident) = @_;

	my $raw_albs = _get_raw_cws_albs($cms_dbh, $alb_ident);

	my $alb_hostnames = {};
	for my $raw_alb ( @$raw_albs ) {
		push @{$alb_hostnames->{$raw_alb->[1]}}, $raw_alb->[0];
	}

	return $alb_hostnames;
}

=head3 store_graphite_data

sends data to carbon for storing

IN:	path: graphite path to use to retrieve the data:
					common.las1.pool.1.memory.used	
	data: arrayref of arrarefs: 
					[ [value1, timestamp1], [value2, timestamp2], ... ]
=cut

sub store_graphite_data {
	my ($path, $data) = @_;

	## create graphite url for inserting the data:
	my $message;

	## send a message (string) "path metric_value epochtime"
	foreach my $datapoint (@$data) {
		$datapoint->[0] = '' if ! defined $datapoint->[0];
		$message = "$path $datapoint->[0] $datapoint->[1]\n";
		$sock->send($message);	
	}
}

=head2 PRIVATE METHODS

=head3 _create_and_pool_metrics_aggr

this is where the pool aggregation and the storing is happening

a) the graphite request url is adjusted for the particular metric being handled
b) graphite is queried and its response is decoded
c) graphite data is stored in a new/existing Whisper file fror that particular datacentre, pool and metric

=cut

sub _create_and_pool_metrics_aggr {
	my ($graphite_info, $graphite_template, 
	    $metric, 	    $pool_info  ) = @_;

	my ($response, $data);

	## the graphite request url is adjusted for the particular metric being handled
	#------------------------------------------------------------------------------
	my $target = join ",", map { "$_.$metric" } @{$pool_info->{vms}};

	## graphite is queried and its response is decoded
	#-------------------------------------------------
	$graphite_template =~ s/TARGET/$target/g;
	eval {
		$response = $ua->get($graphite_template);
		$data = decode_json($response->decoded_content)->[0]{datapoints};
	};
	return if $@;

	## graphite data is stored in a new/existing Whisper file from that particular datacentre, pool and metric
	#---------------------------------------------------------------------------------------------------------
	my $path = "$graphite_info->{prefix}.$pool_info->{dc}.pools.$pool_info->{id}.$metric";
	store_graphite_data($path, $data);

	return $target;
}

=head3 _get_raw_cws_albs

gets hostname and its corresponding datacentre name

=cut

sub _get_raw_cws_albs {
	my ($cms_dbh, $alb_ident) = @_;

	my ($product_short_name,
	    $site_short_name,
	    $site_purpose) = ($alb_ident->{product_short_name},
			      $alb_ident->{site_short_name},
			      $alb_ident->{site_purpose});

	my $sql = "
		SELECT 	host.name       as hostname, 
			datacentre.name as dcname
		FROM host 
		JOIN .....
		JOIN .....
		JOIN .....
		JOIN .....
		WHERE tenant.short_name   = 'cws' 
		AND site.short_name     = '$site_short_name' 
		AND site.purpose        = '$site_purpose' 
		AND product.short_name  = '$product_short_name' ";

	my $rows = $cms_dbh->selectall_arrayref($sql);

}

=head3 _massage_pool_info 

IN:	ALB response for /pools API call

=cut

sub _massage_pool_info {
	my ($pools) = @_;

	my $massaged = {};	

	for my $pool (@$pools) {
		next unless $pool->{status} eq 'active';
		map { $massaged->{ $pool->{id} }{$_} = undef } @{$pool->{servers}};
	}

	return $massaged;
}

=head3 _massage_server_info 

=cut

sub _massage_server_info {
	my ($servers) = @_;

	my $massaged = {};
	foreach my $server ( @$servers ) {
		next unless $server->{status} eq 'active';
		$massaged->{$server->{host}} = undef;
	}

	return $massaged;
}


=head3 _get_raw_pool_vm_info

=cut

sub _get_raw_pool_vm_info {
	my ($cms_dbh, $pool_vms) = @_;

	my $where = join ',', @$pool_vms;

	my $sql = "
		SELECT 	host.name as name, 
			INET_NTOA(interface.ipv4_address) as ipv4_address, 
			host.id as id,
		FROM host 
		JOIN interface ON interface.host=host.id 
		WHERE INET_NTOA(interface.ipv4_address) IN $where ";

	my $rows = $cms_dbh->selectall_arrayref($sql);

}

=head3 _get_pool_info

gets json response from an ALB:

.../pools :
[{"id": "123", "servers": ["12.13.14.15", "13.14.15.16"], "status":"active"},{"id": "234", "servers": ["123.124.125.126"], "status":"inactive"}]

.../servers :
[{"host": "12.13.14.15", "status": "active"},{"host": "123.124.125.126", "status": "inactive"}]

decodes into perl data structure

massages into a more suitable structure

IN:	LWP::UserAgent instance
	ALB connect information
OUT:	undef on failure (+ error printout)
	{	pools => [ qw( $pool1_id $pool2_id ...) ],
		pool_servers => {
				    $pool1_id => [ ip1 ip2 ip3 ...],
				    $pool2_id => [ ip4 ip5 ip6 ...],
				    ...
				    ...
				}
	} 

=cut

sub _get_pool_info {
	my ($cms_dbh, $ua, $alb_info) = @_;

	my $pools = get_alb_pool_response($ua, $alb_info);

	return if ! defined $pools;

	my $pool_hostnames 	= {};
	for my $dcname ( keys %$pools ) {
		for my $pool ( keys %{$pools->{$dcname}} ) {
			my @ips = keys %{$pools->{$dcname}{$pool}};
			$pool_hostnames->{$dcname}{$pool} = _get_vm_host_info($cms_dbh, \@ips);
		}
	}

	return $pool_hostnames;

}

=head3 get_alb_pool_response

gets pool info from Application Load Balancer

IN:	UserAgent object
	alb hashref{
		alb->{ hostnames } ... ALB hostnames 
		alb->{ connect }   ... connect API details
OUT:	decoded ALB response from json to a perl data structure
	[{"id": "123", 
	  "status": "active",
	  "servers": ["12.13.14.15","13.14.15.16"}]},
	 {"id": "234", 
	  "status": "passive",
	  "servers": [{"host": "123.124.125.126"]}]

=cut

sub get_alb_pool_response {
	my ($ua, $alb_info) = @_;

	my @datacentres = keys %{$alb_info->{hostnames}};
	if (! scalar @datacentres) {
        	say "WARNING: No loadbalancers are available";
		return;
	}

	my $pools = {};
	for my $dcname ( @datacentres ) {
	
		my @albs = @{$alb_info->{ hostnames }{ $dcname }};
		
		next unless scalar @albs;
	
		## Get pool information from the first ALB server that is available
		#------------------------------------------------------------------
		my $response;
	
		$response   = get_alb_response($ua, $alb_info, \@albs, 'pools');
		die "ERROR: cannot contact ALB layer for pool information"   if not defined $response;
	
		## selects only active pools
		$pools->{ $dcname } = _massage_pool_info($response);
	
		$response = get_alb_response($ua, $alb_info, \@albs, 'servers');
		die "ERROR: cannot contact ALB layer for server information" if not defined $response;
	
		my $servers = _massage_server_info($response);
	
		for my $pool ( keys %{$pools->{$dcname}} ) {
			for my $pool_server ( keys %{$pools->{$dcname}{$pool}} ) {
				delete $pools->{$dcname}{$pool}{$pool_server} unless exists $servers->{$pool_server};
			}
		}
	}

	return $pools;
}

=head3 get_alb_response

get ALB response for an API request

IN:	UserAgent instance
	loadbalancer HTTP info: schema, port
	arrayref of loadbalancer hostnames (from CMS)
	API call (eg pools or servers)

=cut

sub get_alb_response {
	my ($ua, $alb_info, $albs, $type) = @_;

#vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
## FAKE data for testing before ALB became available in any shape ot form
#vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
#=if ($type eq 'pools') {
#=	return [ 
#=		 { id => 1,
#=		   status => 'active',
#=		   servers => [ '10.10.10.11', '10.10.10.12', '10.10.10.13' ],
#=		 },
#=		 { id => 2,
#=		   status => 'active',
#=		   servers => [ '10.10.10.14', '10.10.10.15' ],
#=		 },
#=	       ];
#=}
#=else {
#=	return [ 
#=		 { 
#=		   host   => '10.10.10.11',
#=		   status => 'active',
#=		 },
#=		 { 
#=		   host   => '10.10.10.12',
#=		   status => 'active',
#=		 },
#=		 { 
#=		   host   => '10.10.10.13',
#=		   status => 'inactive',
#=		 },
#=		 { 
#=		   host   => '10.10.10.14',
#=		   status => 'active',
#=		 },
#=		 { 
#=		   host   => '10.10.10.15',
#=		   status => 'active',
#=		 },
#=	       ];
#=}
#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

	my $response;
	for my $hostname ( @$albs ) {
		my $url = $alb_info->{ connect }{ schema }	. 
			  $hostname;
		$url .= ":" . $alb_info->{ connect }{ port } if $alb_info->{ connect }{ port }; 
		$url  = $url . "/$type"; 				

		$response = $ua->get($url);
		last if $response->is_success;
	}

	if ( ! $response || ! $response->is_success ) {
        	say "ERROR: getting loadbalancer $type information failed: " . $response->status_line;
		return;
	}

     	my $info = decode_json $response->decoded_content;

}

=head3 _get_vm_host_info

=cut

sub _get_vm_host_info {
	my ($cms_dbh, $ips) = @_;
	
	return [] unless scalar @$ips;

	my ($sql, $where, $hostnames);
	
	$where = join ',', map { "'$_'" } @$ips;

	$sql = "SELECT host.name FROM host 
		JOIN   interface ON interface.host=host.id
		WHERE  INET_NTOA(interface.ipv4_address) IN ($where)";

	eval {
		$hostnames = $cms_dbh->selectall_arrayref($sql);
	};
	
	if ($@) {
		say "ERROR: " . $cms_dbh->errstr;
		return;
	}

	return _massage_vm_hostnames($hostnames);
}

=head3 _massage_vm_hostnames

IN:	DBI resultset
OUT:	{ ip1 => $hostname, ... }

=cut

sub _massage_vm_hostnames {
	my ($raw_data) = @_;

	my @hostnames = map { $_->[0] } @$raw_data;
	return \@hostnames;
	
}

sub _create_graphite_period_limit {
	my ($dt) = @_;

	my ($min,$hour, $day,$month,$year,  $limit);
	($min,$hour, $day,$month,$year) = ($dt->min, $dt->hour,
				  	   $dt->day, $dt->month, $dt->year);

	$limit = sprintf("%02d:%02d_%4d%02d%02d", $hour, $min, $year, $month, $day);
}

=head3 _get_metrics_info

=cut

sub _get_raw_metrics_path {
	my ($cmsdash_dbh, $metric_ids) = @_;

	my $metric_data = {};
	foreach my $metric_type ( keys %$metric_ids) {
		my $where = join ',', @{$metric_ids->{$metric_type}};
		my $sql =
			   "SELECT path, aggr_type
			    FROM metric_info
			    WHERE id IN ($where);
			   ";

		my $rows = $cmsdash_dbh->selectall_arrayref($sql);	
		foreach my $row (@$rows) {
			$metric_data->{$metric_type}{$row->[0]} = $row->[1];
		}
	} 
	return $metric_data;	
}

=head3 _create_graphite_url_template 

=cut

sub _create_graphite_url_template {
	my ($graphite_connect, $period_info) = @_;

	my $template = {};

	my $graphite_url_template = $graphite_connect->{ schema } .
				    $graphite_connect->{ host };
	$graphite_url_template .=   ':' . $graphite_connect->{ port } if $graphite_connect->{ port };
	$graphite_url_template .=   '/' . $graphite_connect->{ source } .
				    $graphite_connect->{ period } ;

	$graphite_url_template =~ s/FROM/$period_info->{FROM}/;
	$graphite_url_template =~ s/UNTIL/$period_info->{UNTIL}/;

	$template->{ api_aggr } = "${graphite_url_template}&" . $graphite_connect->{ api_aggr };
	$template->{ api_95 }   = "${graphite_url_template}&" . $graphite_connect->{ api_95 };

	return $template;
}

=head3 _massage_vm_names

massages the cms hostname to be suitable for graphite purposes

IN:	hashref with info for adjusting the VM partial urls for graphite
OUT:	adjusted VM partial urls

=cut

sub _massage_vm_names {
	my ($info, $pool, $pools_info) = @_;

	my $graphite_prefix = $info->{prefix};
	my $dcname          = $info->{dcname};

	my @vms =  map { 
			 if ($graphite_prefix) {
			     "$graphite_prefix.$dcname.$_"
			 } 
			 else {
			     "$dcname.$_"
			 }
		       } @{$pools_info->{$dcname}{$pool}};
	return \@vms;

}
