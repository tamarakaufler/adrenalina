package Helper::XXXreports;

use strict;

=head1 XXXreports

helper methods for the script for reporting on 

=cut

$| = 1;
my $DEBUG = 0;

use strict;
use warnings;
use v5.10.1;

do {
    use Data::Dumper qw(Dumper);
} if $DEBUG;

#$ENV{DBIC_TRACE} = 1;

use FindBin       qw($Bin);
use lib           "$Bin/..";

use YAML::Tiny;
use DBI;
use Net::SNMP;
use XML::Simple;
use LWP::UserAgent;
use HTTP::Request;
use URI::Escape;

use DB_File;
use Fcntl;

use MIME::Lite;

use List::Util qw(max min sum);
use Schema::CmdbDB;

use Exporter;

our (@ISA)     = qw(Exporter);
our @EXPORT_OK  = qw(
            get_config
            get_asset_schema
            get_asset_dbh
            getLoadBalancers
            getInServiceState4MidTower    
            getLiveLogicalTowers
            attachServers
            is_morning
            recordLogTowerXXXServers
            sendEmail
                    );

my %MAP = (
    '8.8.8.8.8.8.8888.1.6'      => 'AD3',
    '8.8.8.8.8.8.8888.1.13.1.1' => 'AS2424',
    '8.8.8.8.8.8.8888.1.13.1.4' => 'AS2424',
    '1.3.6.1.4.1.9.1.888'       => 'ACE'
);

my %OID = (
    'AD3' => {
        'SLB_CUR_CFG_REAL_SERVER_STATE'  => '9.9.9.9.9.9999.9.1.5.2.1.10',     # Returns the current state of each constituent server
        'SLB_CUR_CFG_REAL_SERVER_IPADDR' => '9.9.9.9.9.9999.9.1.5.2.1.2',      # Returns IP Address for each of the constituent servers
        'SLB_NEW_CFG_REAL_SERVER_STATE'  => '9.9.9.9.9.9999.9.1.5.3.1.10',
        'SLB_NEW_CFG_REAL_SERVER_IPADDR' => '9.9.9.9.9.9999.9.1.5.3.1.2',
        'AG_APPLY_CONFIGURATION'         => '9.9.9.9.9.9999.9.1.2.1.2.0',
        'AG_SAVE_CONFIGURATION'          => '9.9.9.9.9.9999.9.1.2.1.1.0',
        'SLB_REAL_SERVER_INFO_STATE'     => '9.9.9.9.9.9999.9.1.9.2.2.1.7',    # Real server info state
        'SLB_CUR_CFG_GROUP_REAL_SERVERS' => '9.9.9.9.9.9999.9.1.5.10.1.2',
        'SLB_CUR_CFG_VIRT_SERVER_IPADDR' => '9.9.9.9.9.9999.9.1.5.5.1.2'
    },    # Virtual Server IP Address

    'AS2424' => {
        'SLB_CUR_CFG_REAL_SERVER_STATE'  => '9.9.9.9.9.9999.9.5.4.1.1.2.2.1.10',
        'SLB_CUR_CFG_REAL_SERVER_IPADDR' => '9.9.9.9.9.9999.9.5.4.1.1.2.2.1.2',
        'SLB_NEW_CFG_REAL_SERVER_STATE'  => '9.9.9.9.9.9999.9.5.4.1.1.2.3.1.10',
        'SLB_NEW_CFG_REAL_SERVER_IPADDR' => '9.9.9.9.9.9999.9.5.4.1.1.2.3.1.2',
        'AG_APPLY_CONFIGURATION'         => '9.9.9.9.9.9999.9.5.1.1.1.2.0',
        'AG_SAVE_CONFIGURATION'          => '9.9.9.9.9.9999.9.5.1.1.1.4.0',
        'SLB_REAL_SERVER_INFO_STATE'     => '9.9.9.9.9.9999.9.5.4.3.1.1.7',
        'SLB_CUR_CFG_GROUP_REAL_SERVERS' => '9.9.9.9.9.9999.9.5.4.1.1.3.3.1.2',
        'SLB_CUR_CFG_VIRT_SERVER_IPADDR' => '9.9.9.9.9.9999.9.5.3.1.6.3.1.3'
    },

    'ACE' => {
        'getState'   => \&getStateXML,
        'getWorking' => \&getWorkingXML,
    }
);

my %g_transl = (
    'OPERATIONAL'  => 2,
    'OUTOFSERVICE' => 3,
    'PROBE-FAILED' => 4
);

my $XML_LB = {};
my %mapcache;


=head3 get_config

=cut

sub get_config ($) {
    my ( $file ) = @_;

    die if not -e $file;

    my $yaml = YAML::Tiny->read( $file );
             
    my $config = {
            asset      => { 
                    dbd     => $yaml->[0]->{ asset }{ dbd },
                    host    => $yaml->[0]->{ asset }{ host },
                    db      => $yaml->[0]->{ asset }{ db },
                    user    => $yaml->[0]->{ asset }{ user },
                    connect => $yaml->[0]->{ asset }{ connect },
                     },
            readcomm  => $yaml->[0]->{ readcomm },
            sysOID    => $yaml->[0]->{ sysOID },
            email      => { 
                    from     => $yaml->[0]->{ email }{ from },
                    to       => $yaml->[0]->{ email }{ to },
                    subject  => $yaml->[0]->{ email }{ subject },
                    smtp     => $yaml->[0]->{ email }{ smtp },
                     },

    };

    $XML_LB = {
                    user     => $yaml->[0]->{ XML_LB }{ user },
                    sequence => $yaml->[0]->{ XML_LB }{ sequence },
          };

    return $config;

}

=head3 get_asset_dbh

IN:    $config->{ CMDB }
OUT:    database handle

=cut

sub get_asset_dbh ($) {
    my ( $connect_data ) = @_;

    my ( $asset_dbd, 
         $asset_host, 
         $asset_db, 
         $asset_user, 
         $asset_connect ) =     (
                    $connect_data->{ dbd },
                    $connect_data->{ host },
                    $connect_data->{ db },
                    $connect_data->{ user },
                    $connect_data->{ connect },
                );
    
       my $dbh = DBI->connect(
                'dbi:' . $asset_dbd .  ':host=' . $asset_host .  ';database=' . $asset_db,
                $asset_user,
                $asset_connect
       ) or die "Cannot connect to $asset_db database: $!";

       $dbh->{mysql_auto_reconnect} = 1;

       return $dbh;

}


=head3 get_asset_schema

IN:    $config->{ CMDB }
OUT:    DBIC schema

=cut

sub get_asset_schema ($) {

    my ( $connect_data ) = @_;

    my ( $asset_dbd, 
         $asset_host, 
         $asset_db, 
         $asset_user, 
         $asset_connect ) =     (
                    $connect_data->{ dbd },
                    $connect_data->{ host },
                    $connect_data->{ db },
                    $connect_data->{ user },
                    $connect_data->{ connect },
                );
    
    my $asset_schema = Schema::CmdbDB->connect("DBI:$asset_dbd:host=$asset_host;dbname=$asset_db", 
                                                $asset_user, $asset_connect) 
                                       or die "Cannot connect to Asset database: $!";
}

=head3 getRawLoadBalancers

  Gets the Load Balancers using the parent of the logical tower
  
   IN: $dbh             Database handle
       $parent          parent of the logical tower ( they're parented to mid-tower, same as the load balancers )
  OUT: array of hashes   

=cut

sub getRawLoadBalancers($) {
    my ( $schema ) = @_;

    my $lbs = [$schema->resultset('Asset')->search({'me.clFK'       => 2,
                                                     'me.name'     => { '-not_like' => '%old%' },
                                                     :%'parent.name' => { '!='        => 'T030'  },   
                                                    },   
                                           {    
                                             'join'     => [   'model', 
                                                               'parent',
                                                             { 'bay' => { 'aisle' => { 'room' => { 'floor' => 'dc' }}}}],
                                             'select'   => [ 'me.name', 'me.ip', 'me.parent', 'parent.name', 'dc.name', 'model.name' ],
                                             'as'       => [ 'name',    'ip',    'parent',    'parentname',   'dcname',  'modelname' ],
                                             'order_by' =>   { -asc  => 'me.name' },
                                           }
                                          )->all];

}

sub getLoadBalancers {
    my ( $schema ) = @_;

    my %midtower = ();

    my $lbs = getRawLoadBalancers( $schema );

    foreach my $lb ( @$lbs ) {
    next unless 
            $lb->get_column('modelname') &&
            $lb->parent              &&
            $lb->parent != -1        &&
            $lb->ip               &&
            $lb->ip =~ /\A[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\z/;
    $midtower{ $lb->parent }{name}       = $lb->name;
    $midtower{ $lb->parent }{dc}         = $lb->get_column('dcname');
    push @{$midtower{ $lb->parent }{models}}, $lb->get_column('modelname');
    push @{$midtower{ $lb->parent }{lbs}}, $lb->ip;
    }

    return \%midtower;
}

=head3 getRawLiveLogicalTowers

=cut

sub getRawLiveLogicalTowers {
    my ( $dbh, $schema ) = @_;

    my $not_yet_live_logtwids = _getRawNotYetLiveLogicalTowerIDs( $dbh );    

        my $search = 
                { '-and'       =>  [ 
                              'ciID'   => { '-not_in'   => $not_yet_live_logtwids  } ,
                              'clFK'   =>   1,
                              'name' => {   like      => "T%" },
                              'name' => { '-not_like' => '%old%' },
                              'parent' => { '!=' =>  -1 } ,
                              'parent' => { '!=' =>   0 } ,
                              'parent' => { '!=' =>   undef } ,
                              'parent' => { '!=' =>  '' } ,
                                   ],
                };

    my $logtowers = [ $schema->resultset('Asset')->search( 
                  $search,
                  {
                      'select'  => [ 'ciID', 'name', 'parent' ],
                  } )->all ];

}

=head3 getLiveLogicalTowers

IN:    dbh
    logtower hashref

OUT:     NA

=cut

sub getLiveLogicalTowers {
    my ( $dbh, $schema, $logtower ) = @_;

    my $logtowers = getRawLiveLogicalTowers( $dbh, $schema );    ## DBIC objects

    foreach my $logtw ( @$logtowers ) {
        $logtower->{ $logtw->ciID } = { name   => $logtw->name, 
                        parent => $logtw->parent };
        
        }
    #print "number of live logtowers: " . scalar keys %$logtower;

}

=head3 recordLogTowerXXXServers

record XXX servers in a BerkleyDB file for later emal notification

IN:    bdb hashref
    $state, 
    $work_state, 
    $datacentre, 
    load balancer type : ACE/ALT
    $logwid, 
    $servers
OUT:    NA

=cut

sub recordLogTowerXXXServers ($$$$$$$$) {
    my ($bdb_hash,   
        $state, $work_state,
        $datacentre, $lb_type, 
        $logname,    $servers,
        $hour) = @_;

    return unless scalar @$servers;

    my $dailyXXXservers  = $bdb_hash->{ daily };
    my $weeklyXXXservers = $bdb_hash->{ weekly };

    print "\t\t$logname, $datacentre : " , scalar @$servers, "\n";

    foreach my $srv ( @$servers ) {
        my $ip   = $srv->{ ip };
        my $name = $srv->{ name };

        my $value = "$datacentre|$lb_type|$logname|$name";

        ## server is IN SERVICE
        if ((defined $state->{ $ip } and $state->{ $ip } == 2) and ($work_state->{ $ip } == 2)) {
            print "\t$name is in service\n";
            _handleISserver( $hour, 
                             $dailyXXXservers, $weeklyXXXservers, 
                             $ip);
        }
        ## server NOT IN SERVICE or IN SERVICE BUT NOT OK
        else {
            if ( not defined $state->{ $ip } ) { 
                $value .= "|0|0";

                print "\t$name LOADBALANCER DOES NOT KNOW ABOUT THE SERVER ($lb_type: 0|0)\n";

            }
            ## server IN SERVICE BUT NOT OK
            elsif ( ($state->{ $ip } == 2) and ($work_state->{ $ip } != 2) ) {
    
                my $work_xxx = ( not defined $work_state->{ $ip } ) ? 0 : $work_state->{ $ip };
                $value .= "|2|$work_xxx";
    
                print "\t$name IS OUT IN SERVICE BUT NOT OK ($lb_type: 2|$work_xxx)\n";
    
            }
            ## server NOT IN SERVICE
            else {
    
                my $xxx      = $state->{ $ip };
                my $work_xxx;
                if ( not defined $work_state->{ $ip } ) {
                    given ($lb_type) {
                        when ('ACE') { $work_xxx = 4 };
                        when ('ACL') { $work_xxx = 0 };
                    }
                }
                else {
                    $work_xxx = $work_state->{ $ip };
                }
                $value .= "|$xxx|$work_xxx";
    
                print "\t$name IS OUT OF SERVICE ($lb_type: $xxx|$work_xxx)\n";
    
            }

            _handleXXXserver($hour, 
                         $dailyXXXservers, $weeklyXXXservers, 
                     $ip, $value);
        }
    } 
}

=head3 is_morning

IN:     hour
OUT:    boolean 0/1

=cut

sub is_morning {
    my ( $hour ) = @_;

    my $morning = 1;
    if ( $hour > 12 ) {
        $morning = 0;
    }

    return $morning;
}

=head3 sendEmail

IN:     $email_info
        $bdb
        $is_weekly

=cut


sub sendEmail ($$;$) {
    my ( $email_info, $bdb, $is_weekly ) = @_;

    my ( $subject, $start, $end ) = (
                            $email_info->{ subject },
                            $email_info->{ start },
                            $email_info->{ end },
                        );

    $subject = $subject . ' - ' . $start;

    # get the info from the BDB file
    my $body = _createEmailContent( $bdb, 
                                    $is_weekly, $start, $end);

    my $msg = MIME::Lite->new( From    => $email_info->{ from },
                               To      => $email_info->{ to },
                               Subject => $email_info->{ subject },
                               Type    => 'text/html',
                               Data    => $body,
                             );

    MIME::Lite->send( 'smtp', $email_info->{ smtp }, Debug => 0 );

    $msg->send() || warn "XXX servers report: No Email";
}

=head3 getRawServers

gets servers in a territory, datacentre or all servers if no $field and $value were supplied

IN:    Catalyst context object or schema object
    
    optional:
        search_by: dc or territory (DBIC asset relationships)
        field ... id/name : optional. If present then servers for the given datacentre will be found

OUT:    arrayref of servers as DBIC objects

=cut

sub getRawServers ($$) {
    my ( $schema, $logtw_ids ) = @_;

    my $search = 
          { 
            '-and'  =>  [ 'name'   => {   like      => "%sss-%-t%" },
                          'name'   => { '-not_like' => '%:%' },
                          'name'   => { '-not_like' => '%old%' },
                          'parent' => { '-in' => $logtw_ids  } ,
                        ],
                '-or'        =>  [  cl  =>   5 ,
                                    cl  =>   11,  ], 
          };
    my $servers = [ $schema->resultset('Asset')->search( 
                    $search,
                    {
                      'select'  => [ 'name', 'ip', 'parent' ],
                    } )->all ];

    return $servers;
    
}

sub attachServers ($$) {
    my ( $schema, $logtower ) = @_;

    my @logtw_ids = keys %$logtower;
    my $servers = getRawServers($schema, \@logtw_ids);

    foreach my $server (@$servers) {
    push @{$logtower->{ $server->parent }{ servers }}, {  name => $server->name, 
                                                          ip   => $server->ip  };
    }

}

=head3 getInServiceState4MidTower

gets in service state from a logical tower from its corresponding load balancer

OUT:    state IN/OUT  hashref
    state working hashref
    lb type ACE/ALT

=cut

sub getInServiceState4MidTower {
    my ($lbs, $config ) = @_; 

    my $readcomm = $config->{readcomm};
    my $sysOID   = $config->{sysOID};

    return unless $readcomm && $sysOID;

    my (%state, %work_state, $lb_type, $lb_type_out); 
    my $failed = 1; 

    my $index = 0;
    foreach my $lb_ip ( @{ $lbs } ) { 
        my ( $sessionread, $type, $error )  = getSessionAndType( $lb_ip, 
                                                                 $readcomm, $sysOID );
    ## this may be an ACE load balancer
    next unless defined $sessionread && defined $type; 

    ## operational state :  IN/OUT (ALTEON)
    #                       OPERATIONAL/OUTOFSERVICE/PROBE-FAILED (ACE)
    my ( $lb_state_in_out, $lb_type, undef ) = getStateActual( $lb_ip, 
                                                               $sessionread, $type, $error,
                                                               $sysOID );
    next unless $lb_state_in_out;
    $failed = 0; 

    ## working state     :  running/failed/disabled (ALTEON)
    #                       OPERATIONAL/OUTOFSERVICE/PROBE-FAILED (ACE)
        my ( $lb_state_work, undef, undef )      = _getWorkingActual( $lb_ip, 
                                     $sessionread, $type, $error,
                                     $sysOID );

    ## 001 is the authoritative load balancer
    if ( ! $index ) {
        @state{ keys %$lb_state_in_out } = values %$lb_state_in_out;
        my (undef, $lb_type_oid) = each %$lb_type;
        $lb_type = ( defined $MAP{ $lb_type_oid } && $MAP{ $lb_type_oid } eq 'ACE') ? 'ACE' : 'ALT';
        $lb_type_out = $lb_type;

        @work_state{ keys %$lb_state_work } = values %$lb_state_work;
    }
    ## 002 result is added only if we did not get it from 001 for the server in question
    else {
        my %additional_state = map { $_ => $lb_state_in_out->{ $_ } } grep { not exists $state{ $_ } } keys %$lb_state_in_out;
        @state{ keys %additional_state } = values %additional_state;
    
        %additional_state    = map { $_ => $lb_state_work->{ $_ } }   grep { not exists $work_state{ $_ } } keys %$lb_state_work;
        @state{ keys %additional_state } = values %additional_state;
    }

    $index++;
    }   

    ( $failed ) ? return : return (\%state, \%work_state, $lb_type_out);

}

#=========================================== PRIVATE METHODS ===========================================

=head3 _getRawNotYetLiveLogicalTowerIDs

IN:     database handle
OUT:    arraref of DBI hashrefs

=cut

sub _getRawNotYetLiveLogicalTowerIDs {
    my ( $dbh ) = @_;

    my $sql = "select .... "
    my $logtowers = $dbh->selectall_arrayref($sql, { Slice => {} });

    my @not_yet_live_logtwids = map { $_->{ ciID } } @$logtowers;    

    return \@not_yet_live_logtwids;

}


=head3 _handleXXXserver

handles servers that are found out of service when the script runs

IN:    hour
    dailyXXXservers  href
    weeklyXXXservers href
    IP Address
    datacentre|logtower name|server name|operational state|working state

=cut

sub _handleXXXserver {
    my ($hour, 
        $dailyXXXservers, $weeklyXXXservers, 
        $ip, $value) = @_;

    my $morning = is_morning($hour);

    if ( $morning ) {
        if ( exists $dailyXXXservers->{ "${ip}am" } ) {
            print "\t\t\tadding to weeklyXXXservers (am)\n";
            $weeklyXXXservers->{ $ip } = $value;
            delete $dailyXXXservers->{ "${ip}am" };
        }
        else {
            print "\t\t\tadding to am dailyXXXservers\n";
            $dailyXXXservers->{ "${ip}am" } = $value;
        }
    }
    else {
        if ( exists $dailyXXXservers->{ "${ip}pm" } ) {
            print "\t\t\tadding to weeklyXXXservers (pm)\n";
            $weeklyXXXservers->{ $ip } = $value;
            delete $dailyXXXservers->{ "${ip}pm" };
        }
        else {
            print "\t\t\tadding to pm dailyXXXservers\n";
            $dailyXXXservers->{ "${ip}pm" }  = $value;
        }
    }
}

=head3 _handleISserver

handles servers that are found in service when the script runs

IN:    hour
    dailyXXXservers  href
    weeklyXXXservers href
    IP Address
    datacentre|logtower name|server name
OUT:    NA

=cut

sub _handleISserver {
    my ($hour, 
        $dailyXXXservers, $weeklyXXXservers, 
        $ip) = @_;

    my $morning = is_morning($hour);

    if ( $morning ) {
        return unless exists $dailyXXXservers->{ "${ip}pm" };
        print "\t\t\tdeleting pm dailyXXXservers\n";
        delete $dailyXXXservers->{ "${ip}pm" };
    }
    else {
        return unless exists $dailyXXXservers->{ "${ip}am" };
        print "\t\t\tdeleting am dailyXXXservers\n";
        delete $dailyXXXservers->{ "${ip}am" };
    }
}

=head3 _createEmailContent

IN:    hashref of data
    dao of week
    hour
    is_weekly
    start date of the week reported on
    end   date of the week reported on
=cut

sub _createEmailContent ($;$$$) {    
    my ( $bdb, 
         $is_weekly, $start, $end ) = @_;
         

    my @bgcolor_alt = ( '#808080', '#afaafa', '#E2725B'  );
    my %bgcolor          = ( 
                 '00' => '#808080', 
                 '34' => '#afaafa', 
                 '33' => '#afaafa', 
                 '23' => '#E2725B', 
                 '44' => '#E2725B', 
                   );

    my %why_xxx          = ( 
                 0 => 'Cannot reach loadbalancer', 
                 2 => 'In loadbalancer', 
                 3 => 'Taken out of load balancer',
                 4 => 'PROBE-FAILED',    ## only for ACE
                 );
    my %why_work_xxx_ace = ( 
                 0 => 'Cannot reach loadbalancer', 
                 2 => 'OPERATIONAL', 
                 3 => 'OUTOFSERVICE',
                 4 => 'PROBE-FAILED', 
                 );
    my %why_work_xxx_alt = ( 
                 0 => 'Cannot reach loadbalancer', 
                 2 => 'Running', 
                 3 => 'Failed', 
                 4 => 'Disabled', 
                 );

    ## create a suitable structure 
    my @keys = keys %$bdb;
    my %content = ();
    my $srv_count = 0;
    my (%why_work_xxx);

    foreach my $key ( @keys ) {
        my ($dc, $lb_type, $logtower, $server, $xxx, $work_xxx) = split /\|/, $bdb->{ $key };
        
        $srv_count++ if not exists $content{ $dc }{ $logtower }{ $server };

        my ($bgcolor);
        if ($lb_type eq 'ACE') {
            %why_work_xxx = %why_work_xxx_ace;
        }
        else {
            %why_work_xxx = %why_work_xxx_alt;
        }

        $content{ $dc }{ $logtower }{ $server }{ lb_type }    = $lb_type;
        $content{ $dc }{ $logtower }{ $server }{ state }      = $xxx      || 0;
        $content{ $dc }{ $logtower }{ $server }{ state_text } = $why_xxx{$xxx}                || 'Unknown';
        $content{ $dc }{ $logtower }{ $server }{ work_state } = $work_xxx || 0;
        $content{ $dc }{ $logtower }{ $server }{ work_state_text } = $why_work_xxx{$work_xxx} || 'Unknown';;

    }

    my $date = scalar localtime(time);

    my $body = "<h3>Out of Service Servers on $date</h3>\n";
    if ( $is_weekly ) {
        $body   .= "<h4>$srv_count servers were Out of Service for more than 24 hours <br />from $start to $end</h4>\n";
    }
    else {
        $body   .= "<h5>There are $srv_count servers currently Out of Service</h5>\n";
    }

    $body   .= "<table border=1 style='width: 950px; background: #FFFFCF;'>\n";
    $body   .= "<tr><th><b>Server</b></th><th><b>Operational state</b></th><th><b>Working state</b></th><th><b>LB Type</b></th><th><b>Logical Tower</b></th><th><b>Datacentre</b></th></tr>\n";

    my $xxx_records = ();
    my @sorted_dcs = sort { $a cmp $b } keys %content;
    foreach my $dc ( @sorted_dcs ) {
        my @sorted_logtowers = sort { $a cmp $b } keys %{$content{$dc}};
        foreach my $logtower ( @sorted_logtowers ) {
            my @sorted_servers = sort { $a cmp $b }  keys %{ $content{$dc}{$logtower} };
            foreach my $server ( @sorted_servers ) {
                my $lb_type    = $content{$dc}{$logtower}{$server}{lb_type};
                my $state      = $content{$dc}{$logtower}{$server}{state};
                my $work_state = $content{$dc}{$logtower}{$server}{work_state};
                my $state_text      = $content{$dc}{$logtower}{$server}{state_text};
                my $work_state_text = $content{$dc}{$logtower}{$server}{work_state_text};

                my $bgcolor    = $bgcolor{"$state$work_state"} || '#FFFFCF';
                my $line = "<tr bgcolor='$bgcolor'><td>$server</td><td>$state_text</td><td>$work_state_text</td><td>$lb_type</td><td>$logtower</td><td>$dc</td></tr>\n";
                $body   .= $line;
            }
            
        }
    }

    $body .= "</table></body></html>\n";
    
}

#==============================================================================================================================
#===========================    METHODS FOR CONTACTING THE LOAD BALANCERS TO GET INFO ON SERVERS IN/OUT   =====================
#==============================================================================================================================

## REMOVED METHODS .....

=head3 _flatten_further

=cut

sub _flatten_further {
    my ($key, $hashref) = @_; 

    if ( ref $hashref->{exec_command}->{xml_show_result}->{xml_show_rserver}->{rs_entry}->{$key} eq 'HASH' ) { 
            if ( ! exists $hashref->{exec_command}->{xml_show_result}->{xml_show_rserver}->{rs_entry}->{$key}->{rs_entry} ) { 

                    $hashref->{exec_command}->{xml_show_result}->{xml_show_rserver}->{rs_entry}->{$key}->{rs_entry} =
                    {   
                        address  => $hashref->{exec_command}->{xml_show_result}->{xml_show_rserver}->{rs_entry}->{$key}{address},
                        rs_state => $hashref->{exec_command}->{xml_show_result}->{xml_show_rserver}->{rs_entry}->{$key}{rs_state},
                        rs_port  => $hashref->{exec_command}->{xml_show_result}->{xml_show_rserver}->{rs_entry}->{$key}{rs_port},
                        rs_sfarm => $hashref->{exec_command}->{xml_show_result}->{xml_show_rserver}->{rs_entry}->{$key}{rs_sfarm},
                    };  
            }   
    }   
    else {
            $hashref->{exec_command}->{xml_show_result}->{xml_show_rserver}->{rs_entry}->{$key} = 
                    { rs_entry => $hashref->{exec_command}->{xml_show_result}->{xml_show_rserver}->{rs_entry}->{$key}[0] };

    }   

    return $hashref;
}

sub _makeRequest {
    my ( $hostname, $command ) = @_;

    my $ua = new LWP::UserAgent;

    my $retry = 2;    # Increase this to a more useful value before putting live

    my $response;
    while ( $retry-- ) {
        $ua->credentials( "$hostname:10443", "Authentication", $XML_LB->{user}, $XML_LB->{sequence} );

        my $req = HTTP::Request->new( POST => "https://$hostname:10443/bin/xml_agent" );

        my $xml = $command;
        $xml = "xml_cmd=" . uri_escape($xml);

        $req->header( 'Content-Length' => length($xml) );
        $req->header( 'Content-Type'   => 'application/x-www-form-urlencoded' );
        $req->content($xml);

        $response = $ua->request($req);

        last if ( ( $response->is_success ) || ( $response->status_line =~ /^401/ ) );
    }

    return { error => 401 } if ( $response->status_line =~ /^401/ );

    return _parseToHash($response);
}

sub _parseToHash {
    my ($response) = @_;

    my $xs = XML::Simple->new();

    my $hash = $xs->XMLin( $response->content );

    if (   ( ( defined $hash->{exec_command}->{status}->{text} ) && ( $hash->{exec_command}->{status}->{text} !~ /XML_CMD_SUCCESS/i ) )
        || ( !defined $hash ) )
    {
        return { error => $hash->{exec_command}->{status}->{code} };
    }

    return $hash;

}

#==============================================================================================================================

1;
