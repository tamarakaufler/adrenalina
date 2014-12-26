package MyApp::Controller::Helper::SQL;

=head1 MyApp::Controller::Helper:SQL

Library of helper methods for the SQL related API controller

=cut

use strict;
use warnings;
use 5.010;

use DBI;
use JSON qw(to_json from_json);
use URI::Escape;

## For DBIC debug
##---------------
#$ENV{ DBIC_TRACE } = 1;

use FindBin       qw($Bin);
use lib           "$Bin/../../..";
use parent 'Exporter';
our @EXPORT_OK =  qw(
                        process_GET_request
                    );

=head2 Exported methods

=head3 process_input_data

IN:    Catalyst object
OUT:    result of the sql queries (arrayref of arrayrefs)

=cut

sub process_GET_request {
    my ($c) = @_;

    my $queries     = _process_user_input($c);
    my $dbh         = _get_dbh($c->model('MyAppDB')->schema->storage->connect_info);
    my $raw_results = _process_db_query($dbh, $queries);
    my $results     = _massage_raw_output($raw_results);
}

=head2 Private methods

=head3 _process_user_input

=cut

sub _process_user_input {
    my ($c) = @_;

    my @queries = ();

    ## url params
    @queries    = @{$c->stash->{ url_params }} if exists $c->stash->{ url_params };

    ## -T/-d : file with one sql query per line
    my @lines = ();
    if ( ref $c->req->data eq 'HASH' ) {
        @lines = keys %{$c->req->data};
    }
    elsif ( ref $c->req->data eq 'ARRAY' ) {
        @lines = @{$c->req->data};
    }

    ## be paranoid: allow only asset CRUD
    push @queries, grep { 
                  $_ !~ /insert/i &&
                  $_ !~ /update/i && 
                  $_ !~ /delete/i 
                } @lines;

    return \@queries;
}

=head3 _get_dbh

IN:     db connection info
OUT:    database handle

=cut

sub _get_dbh {
    my ($connect_info) = @_;
    $connect_info = $connect_info->[0];

    my ($dsn, $user, $password) = ($connect_info->{dsn}, $connect_info->{user}, $connect_info->{password});
    my $dbh;
    eval {
        $dbh = DBI->connect($dsn, $user, $password) or die $DBI::errstr;
    };
    if ( $@ ) {
        return { 
             error => { status  => 'status_bad_request',
                    message =>  "$@ : $DBI::errstr" } 
               };
    }

    return $dbh;
}

=head3 _process_db_query

IN:     database handle
        arrayref of SQL queries
OUT:    arrayref of massaged database results, suitable for API output

=cut

sub _process_db_query {
    my ($dbh, $sql_queries) = @_;

    my @massaged_output = ();
    my @query_output    = ();

    eval {
        foreach my $query ( @$sql_queries ) {
            my $rows = $dbh->selectall_arrayref($query, {Slice => {}});
            my $query_output = _massage_raw_output($rows);

            push @massaged_output, { "$query (" . scalar @$query_output . ") " => $query_output };
        }
    };
    if ( $@ ) {
        return _error_message($@);
    }

    return \@massaged_output;
}

 
=head3 _massage_raw_output

process db output for one query

IN:     DBIC rows ............ as an arrayref
OUT:    massaged structure ... arrayref of hashrefs of data 

=cut

sub _massage_raw_output {
    my ($db_rows) = @_;

    my @massaged = ();

    eval {
        foreach my $row (@$db_rows) {
            my %entity   = ();
            foreach my $attrib ( keys %$row ) {
                $entity{ $attrib } = $row->{ $attrib };        
            
            }        
            push @massaged, \%entity;
        }
    };
    if ( $@ ) {
        return _error_message($@);
    }


    return \@massaged;
}

=head3 _massage_raw_output

IN:     error message (usually $@)
OUT:    RESTful hashref response
 
=cut

sub _error_message {
    my ($message) = @_;

    return { 
             error => { status  => 'status_bad_request',
                        message =>  "error: problem with processing the SQL query - " . 
                                     substr($message, 0, 255) . ' ...'} 
           };
}

1;
