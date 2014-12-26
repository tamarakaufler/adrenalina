package MyApp::Controller::DeepDive::Helper::ReqProcessing;

=head1 MyApp::Controller::DeepDive::Helper::ReqProcessing

Library of helper methods for the Asset realated controllers (GUI, API)

=cut

## Enable some nice features
##--------------------------
use strict;
use warnings;
use 5.010;

use JSON qw(to_json from_json);

$ENV{ DBIC_TRACE } = 1;

use FindBin       qw($Bin);
use lib           "$Bin/../../..";

## This module is a subclass of the Exporter
##------------------------------------------
use parent 'Exporter';
our @EXPORT_OK = qw(
			process_GET_request
			process_PUT_request
			process_POST_request
			process_DELETE_request
		   );


use FindBin qw($Bin);
use lib "$Bin/../../../..";

use MyApp::Controller::Helper::CRUD     qw(
                                         check_related_records
                                         check_purge_ok
					 create_limit
					 limit_output
                                      );
use MyApp::Controller::Helper::DeepDive qw(
					error_exists
				      );

=head1 Exported methods

=head2 process_GET_request

processes GET request

IN:	Catalyst object

=cut

sub process_GET_request {
    my ( $c ) = @_;

    my ($error);
    my $processed4search = $c->stash->{ processed4search };

    my ($limit, $limit_dbic, $selects, $as, $limit_joins);

    if ($c->stash->{'limit'}) {
	$limit = create_limit($c->stash->{'limit'}, $processed4search->[-1][0]);

	my $schema = $c->cache->get($c->session->{'object_type'});
	($selects, $as) = limit_output( $schema, $limit, 'dive');

    	$limit_dbic = { 	
			'select' 	=> $selects, 
			'as'		=> $as, 
		      };

	delete $c->stash->{ limit };
    }

    my $table    = $c->session->{'crud_type'};
    my $schema   = $c->model("MyAppDB::$table");

    my $found_rs = _get_raw_result($schema, $table, $c->session->{ deleted_flag }, 
				   $processed4search, $limit_dbic ); 

    $error = error_exists($found_rs);
    return $error if $error;

    if ( defined $found_rs && $found_rs->count ) {
	my $object_table = $c->session->{'object_type'};
        my @columns      = $c->model("MyAppDB")->source($object_table)->columns;

        return _massage_dbic_GET_result( $table, $c->session->{'object_table'},
					\@columns, $found_rs, $c->session->{ show_deleted }, $limit_dbic );    
    }
    else {

	$error->{ status }  = 'status_not_found';
	$error->{ message } = "No record related to search on table [" . $c->session->{object_type} . "]";

	return { error => $error };
    }
}

=head3 process_DELETE_request

process DELETE request

  IN: $c		Catalyst Context Object
 OUT:

=cut

sub process_DELETE_request {
    my ( $c, $delete_data ) = @_;

    my $error;
    
    my $table              = $c->session->{'crud_type'};
    my $processed4search   = $c->stash->{processed4search};

    my $object_type        = $processed4search->[-1]->[0];
    my $search->{search}   = $processed4search->[-1]->[1];

    my $schema		   = $c->model("MyAppDB::$table");
    my $found_rs           = _get_raw_result( $schema, $table, $c->session->{deleted_flag}, 
					      $processed4search );
    $error = error_exists($found_rs);
    return $error if $error;
    
    if ( defined $found_rs && $found_rs->count ) {

        my $object_table = $c->session->{'object_type'};
        my @columns      = $c->model("MyAppDB")->source( $object_table )->columns;

        # check to make sure there are no related records which will be orphaned or 
        # where referential integrity will be compromised
        my $res = $found_rs->result_source;
        my $failArrRef = check_related_records( $c, $res->source_name, $search );

        # and fail stipulating which tables are affected if this is the case
        if ( @{ $failArrRef } ) {

            $error->{ status  } = 'status_bad_request';
            $error->{ message } = "Related records in following tables must be deleted first: ".
                                  "( ".join(", ", @{ $failArrRef } )." )";
                                  
            return { error => $error };
        }
        else {
        
            my $deleted_objects = _delete_related_objects( $c, $found_rs, $object_type, $delete_data );

    	    $error = error_exists($deleted_objects);
    	    return $error if $error;

            return _massage_dbic_DELETE_result( $table,   $c->session->{'object_table'}, 
					       \@columns, $deleted_objects, $c->session->{ show_deleted } );
        }
    }
    else {

        $error->{ status }  = 'status_not_found';
        $error->{ message } = "No record related to search on table [$table]";

        return { error => $error };
    }
}


=head3 process_PUT_request

process PUT request

  IN: $c		Catalyst Context Object
      $update_data	Data to be applied for updated
 OUT:      

=cut

sub process_PUT_request {
    my ( $c, $update_data ) = @_;

    my $error;

    my $table    	    = $c->session->{'crud_type'};
    my $processed4search    = $c->stash->{ processed4search };

    my $object_type         = $processed4search->[-1]->[0];
    
    my $schema   	    = $c->model("MyAppDB::$table");
    my $found_rs            = _get_raw_result( $schema, $table, $c->session->{ deleted_flag }, $processed4search );

    $error = error_exists($found_rs);
    return $error if $error;

    if ( defined $found_rs && $found_rs->count ) {
        my $object_table    = $c->session->{'object_type'};
        my @columns         = $c->model("MyAppDB")->source($object_table)->columns;

        my $updated_objects = _update_related_objects( $found_rs, $object_type, $update_data );
        my $error = error_exists($updated_objects);
        return $error if $error;

        return _massage_dbic_PUT_result( $table,   $c->session->{'object_table'},
					\@columns, $updated_objects, $c->session->{ show_deleted } );
    }
    else {

        $error->{ status }  = 'status_not_found';
        $error->{ message } = "No record related to search on table [$table]";

        return { error => $error };
    }


}

=head3 process_POST_request

process POST request to create a resource

IN:	
OUT:	if success : number of created records
	if failure : { error => { status  => ...,
				  message => ... } }

=cut

sub process_POST_request {
    my ( $c, $create_data ) = @_;

    my $error;

    my $table    	 = $c->session->{'crud_type'};
    my $processed4search = $c->stash->{ processed4search };
    
    my $object_type      = (pop @{$processed4search})->[0];

    my $schema   = $c->model("MyAppDB::$table");
    my $found_rs = _get_raw_result( $schema, $table, $c->session->{ deleted_flag }, $processed4search ); 

    $error = error_exists($found_rs);
    return $error if $error;

    unless ( defined $found_rs && $found_rs->count ) {
	$error->{ status }  = 'status_not_found';
	$error->{ message } = "No record found in table [$table] using for this search";
	return { error => $error };
    }

    my $created_objects  = _create_related_objects($found_rs, $object_type, $create_data);
    $error = error_exists($created_objects);
    return $error if $error;

    my $show_deleted;
    my $object_table = $c->session->{'object_type'};
    my @columns      = $c->model("MyAppDB")->source($object_table)->columns;
    my $massaged2friendly = _massage_dbic_POST_result($table, $c->session->{'object_table'},
						     \@columns, $created_objects, $c->session->{ deleted_flag });
    
}

##====================================================================================================

=head2 Private methods

=head3 _get_raw_result

IN:	$schema, 
	$table, 
	$deleted_flag,
	$request_data (arrayref of arrayrefs)
	limit info href: { select => ...,
			   as     => ...,
			 }
OUT:	DBIC resultset or { error => { 	status  => ...,
					message => ...}
				     }
=cut

sub _get_raw_result {
    my ( $schema, $table, $deleted_flag, $request_data,
	 $limit_info ) = @_; 

    my ($found_rs, $error);

    eval {
	my $search_part = shift @$request_data;

	my $table       = $search_part->[0]; 
	$search_part->[1]->{ 'me.deleted_flag' } = 'n'  unless defined $deleted_flag;
        $found_rs       = $schema->search( $search_part->[1] );

	my $attrib = {};
	while ( @$request_data ) {
		$search_part = shift @$request_data;

		if (! scalar @$request_data) {
			$attrib = { 
				      select 	=> $limit_info->{ select },
				      as 	=> $limit_info->{ as },
				  };
		}

		## we may want to see deleted objects
		##-----------------------------------
		$search_part->[1]->{ $search_part->[0] . '.deleted_flag' } = 'n'  unless $deleted_flag;
        	$found_rs    = $found_rs->search_related( $search_part->[0], $search_part->[1], $attrib );

	}
    };

    if ( $@ ) {
	$error->{ status }  = 'status_bad_request';
	$error->{ message } = "Error using specified table [$table] in MyApp DB: " . substr( $@, 0, 255 );
	return { error => $error };
    }

    return $found_rs;
}

=head3 _massage_dbic_GET_result

process DBIc resultset into a Perl structure that can be output in the JSON format

IN:	DBIc resultset
OUT:	hashref keyed on asset name with relevant information

=cut

sub _massage_dbic_GET_result {
	my ( $table, $object_db_table, 
	     $columns,  $rs, $deleted_flag, $limit_info ) = @_;

	my $output = [];
	my %uniq = ();

        eval {
        	while ( my $row = $rs->next ) {
			my $row_attribs = _gather_output($object_db_table,
							 $row, $columns, $deleted_flag, $limit_info);
			if ( ! $limit_info && ! exists $uniq{ $row_attribs->{ id } } ) {
				$uniq{ $row_attribs->{ id } } = undef; 
			}
			push @$output, $row_attribs;
        	}
        };
    
        if ( $@ ) {
		my ($error);
    		$error->{ status }  = 'status_bad_request';
    		$error->{ message } = "Error using specified table [$table] in MyApp DB: " . substr( $@, 0, 255 );
    		return { error => $error };
        }

	return $output;
}

=head3 _delete_related_objects

  IN:   Resultset of the objects that satisfy the url search criteria
        Object type: the relationship that points to the table containing items to delete
 OUT:   Arrayref of deleted DBIC objects

=cut
  
sub _delete_related_objects {
    my ( $c, $found_rs, $object_type, $delete_data ) = @_;

    my $deleted_data = [];
    my $deleted_count = 0;

    eval {
        while ( my $row = $found_rs->next ) {
            my $deleted = $row->update( { deleted_flag => 'y' } );
            push @{ $deleted_data }, $deleted;
              
            $deleted_count++;
        }
    };

    return $deleted_data;
}

=head3 _update_related_objects

  IN:	Resultset of the objects that satisfy the url search criteria
        Object type: the relationship that indicated what objects to update
        Arrayref: information for all the objects to be updated
 OUT:   Arrayref of updated DBIC objects
 
=cut

sub _update_related_objects {
    my ( $found_rs, $object_type, $update_data ) = @_;

    my ( $updated_record, $error );
    my $updated_data = [];

    my $updated_count = 0;
    eval {
        while ( my $row = $found_rs->next ) {
            foreach my $up_data ( @{ $update_data } ) {
                my $updated_record = $row->update( $up_data );
                push @{ $updated_data }, $updated_record;

                $updated_count++;
            }
        }
    };

    if ( $@ ) {
        return { error => { status  => 'status_bad_request',
                            message => "Error updating new $object_type records: " .substr( $@, 0, 255 ) . "..." } };
    }

    return $updated_data;
}

=head3 _create_related_objects

IN:	resultset of the objects that satisfy the url search criteria
	object_type: the relationship that indicated what objects to create
	arrayref: information for all of the objects to be stored

OUT:	arrayref of created DBIC objects

=cut

sub _create_related_objects {
    my ( $found_rs, $object_type, $create_data ) = @_;

    my ($new_record, $error);
    my $created_data = [];

    my $created_count = 0;
    eval {
	while ( my $row = $found_rs->next ) {
       		foreach my $new_data (@$create_data) {
    			my $new_record = $row->create_related($object_type, $new_data);
			push @$created_data, $new_record;
			$created_count++;
		}
    	}
    };

    if ( $@ ) {
	return { error => { status  => 'status_bad_request' , 
			    message => "Error creating new $object_type records: " .substr( $@, 0, 255 ) . "..." } };
    }

    return $created_data;
}


=head2 _massage_dbic_PUT_result

process DBIc resultset into a Perl structure that can be output in the JSON format

  IN: Table name
      ArrayRef of columns
      Dbic Objects
      Deleted Flag    	
 OUT: Human readable $output

=cut

sub _massage_dbic_PUT_result {
    my ( $table,   $object_db_table,
	 $columns, $dbic_objects, $deleted_flag ) = @_;
    
    my $output = [];
    $dbic_objects = ( ref($dbic_objects) eq 'ARRAY' ) ? $dbic_objects : [$dbic_objects];
    
    eval {
    	foreach my $object ( @$dbic_objects ) {
            my $object_attribs = _gather_output( $object_db_table, 
						 $object, $columns, $deleted_flag );
	    push @$output, $object_attribs;
        }
    };
    
    if ( $@ ) {
        my ( $error );
     	$error->{ status }  = 'status_bad_request';
    	$error->{ message } = "Error using specified table [$table] in MyApp DB: " . substr( $@, 0, 255 );
    
    	return { error => $error };
    }

    return $output;    
}

=head3 _massage_dbic_DELETE_result

  IN: Table name
      ArrayRef of columns
      Dbic Objects
      Deleted Flag    	
 OUT: Human readable $output

=cut

sub _massage_dbic_DELETE_result {
    my ( $table,   $object_db_table, 
	 $columns, $dbic_objects, $deleted_flag ) = @_;
    
    my $output = [];
    $dbic_objects = ( ref($dbic_objects) eq 'ARRAY' ) ? $dbic_objects : [$dbic_objects];
    
    eval {
        foreach my $object ( @{ $dbic_objects } ) {
            my $object_attribs = _gather_output( $object_db_table,
						 $object, $columns, $deleted_flag );
            push @{ $output }, $object_attribs;
        }
    };
    
    if ( $@ ) {
        my ( $error );
        $error->{status}  = 'status_bad_request';
        $error->{message} = "Error using specified table [$table] in MyApp DB: ". substr( $@, 0, 255 );
        
        return { error => $error };
    }

    return $output;
}

=head3 _massage_dbic_POST_result

process DBIc resultset into a Perl structure that can be output in the JSON format

IN:	arrayref of DBIc objects
OUT:	hashref keyed on asset name with relevant information

=cut

sub _massage_dbic_POST_result {
	my ( $table,   $object_db_table, 
	     $columns, $dbic_objects, $deleted_flag ) = @_;

	my $output = [];
        eval {
        	foreach my $object ( @$dbic_objects ) {
			my $object_attribs = _gather_output( $object_db_table, 
							     $object, $columns, $deleted_flag );
			push @$output, $object_attribs;
        	}
        };
    
        if ( $@ ) {
		my ($error);
    		$error->{ status }  = 'status_bad_request';
    		$error->{ message } = "Error using specified table [$table] in MyApp DB: " . substr( $@, 0, 255 );
    
    		return { error => $error };
        }
	return $output;
}

=head3 _gather_output

creates a perl structure for JSON/YAML etc output

IN:	object db table name (eg asset)
	DBIC object
	table columns
	deleted flag
	limit_info: {
		    }
	arrayref of attributes to output

OUT:	arrayref of hashrefs

=cut

sub _gather_output {
	my ($object_db_table, 
	    $object, $columns, $deleted_flag, $limit_info) = @_;

	my $object_attribs = {};

	if ( $limit_info ) {
		my ($selects, $as) = ($limit_info->{'select'}, $limit_info->{'as'});
		my $i = 0;
		foreach my $attrib (@$selects) {
			my $friendly_attrib = $as->[$i];
			my $value = $object->get_column($friendly_attrib);
			$friendly_attrib =~ s/_fk//;

			$object_attribs->{ $friendly_attrib } =  $value; 

			$i++;
		}
	}
	else {
		foreach my $attrib (@$columns) {
			next if $attrib eq 'deleted_flag' && not $deleted_flag;
	
			my $friendly_attrib = $attrib;
			my $value = $object->$attrib;
	
			if ($friendly_attrib =~ /_fk/) {
				$value = $object->{ _column_data }{ $attrib };
				$friendly_attrib =~ s/_fk//;
				$friendly_attrib = 'parent' if $friendly_attrib eq $object_db_table;
			}
			$object_attribs->{ $friendly_attrib } =  $value; 
		}
	}
	return $object_attribs;
}

1;
