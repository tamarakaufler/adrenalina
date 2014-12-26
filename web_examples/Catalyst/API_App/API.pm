package MyApp::Controller::DeepDive::API;
use Moose;
use namespace::autoclean;

BEGIN {extends 'Catalyst::Controller'; }

use 5.010;
use FindBin qw($Bin);
use lib "$Bin/../../..";

BEGIN {extends 'Catalyst::Controller::REST'; }
__PACKAGE__->config(default => 'application/json');

use MyApp::Controller::Helper::DeepDive qw(
					process_url_params
					process_PUT_POST_DELETE_request_data
					process_search_options
					throws_error
					);
use MyApp::Controller::Helper::CRUD qw(
					get_available_entity_types
                                        get_table
                                        check_auth
					process_crud_type
                                        massage_req2search_params
                                        massage_req2update_params
                                    );
use MyApp::Controller::DeepDive::Helper::ReqProcessing qw(
                                        process_GET_request
                                        process_POST_request
                                        process_PUT_request
                                        process_DELETE_request
                                     );


=head1 NAME

MyApp::Controller::DeepDive::API - Catalyst Controller

=head1 DESCRIPTION

Catalyst Controller.

=head1 METHODS

=cut


=head2 index

=cut

sub index :Path :Args(0) {
    my ( $self, $c ) = @_;
}

sub api_list  :Path('/dive')   :ActionClass('REST') {
    my ( $self, $c, @url_params ) = @_;

    $c->response->headers->header( 'content-type' => 'application/json' );

    ## stash away url_params in case we need to redirect to CRUD API, if listing just crud_type entities
    ## and not relationships
    ##--------------------------------------------------------------------------------------------------
    my @url_params4crud_api = @url_params;

    my ( $success, $status, $message );

    ## Authentication
    #----------------
    if (! defined check_auth( $c, \$status, \$message ) ) {
    
        $self->$status(
                        $c,
                        message => $message
                      );
        $c->detach();
    }

    ## Listing of available object types
    #-----------------------------------
    if ( ! scalar @url_params) {
	my $response_data = get_available_entity_types($c);
	
    	$self->status_ok(
        	                $c,
                	        entity => $response_data,
                    	);
        $c->detach();
    }
    if ( scalar @url_params == 1 ) {
        $c->detach("/crud/api/api_list", [$url_params[0]]);
    }


    ## This determines which object we are doing the crud for
    ## ------------------------------------------------------
    ## (object_type, ie what related objects we are going to create is stored later 
    ##  after processing the url options)
    $c->session->{crud_type} = get_table($url_params[0]);
    $c->cache->remove($c->session->{crud_type});

    my $schema_info;
    if ( ! ($schema_info = $c->cache->get($c->session->{crud_type}) ) ) {

	process_crud_type( $c );
	$schema_info = $c->cache->get($c->session->{crud_type});
    }

    ## limiting output options
    #-------------------------
    if (exists $c->req->params->{ limit }) {
    	$c->stash->{limit} = $c->req->params->{ limit };
	delete $c->req->params->{ limit };
    }

    my $processed_params = process_url_params( $c, \@url_params );
    throws_error($self, $c, $processed_params);

    ## we are not dealing with a relationship request, so need to redirect to CRUD API
    ## -------------------------------------------------------------------------------
    if (scalar @$processed_params == 1) {
            #say STDERR "process_url_params: this request diverted to CRUD API";
            $c->detach('/crud/api/api_list', [@url_params4crud_api]);
    }   

    my $processed4search = process_search_options( $c, $processed_params );
    throws_error($self, $c, $processed4search);

    $c->stash->{processed4search}  = $processed4search;

}

=head2 api_list_GET

=cut

sub api_list_GET {
    my ( $self, $c ) = @_;

    my $response_data = process_GET_request( $c );
    throws_error($self, $c, $response_data);

    $c->stash->{ response_data } = $response_data;
    $self->status_ok(
                        $c,
                        entity => $response_data,
                    );
}

=head2 api_list_PUT

update a resource

=cut

sub api_list_PUT {
    my ( $self, $c ) = @_;

    my $update_data = process_PUT_POST_DELETE_request_data( $c );
    throws_error($self, $c, $update_data);

    my $response_data = process_PUT_request( $c, $update_data );
    throws_error($self, $c, $response_data);

    $c->stash->{ response_data } = $response_data;
    $self->status_ok(
                        $c,
                        entity => $response_data,
                    );
}

=head2 api_list_POST

create related resources for a crud_type

=cut

sub api_list_POST {
    my ( $self, $c ) = @_;

    my $create_data = process_PUT_POST_DELETE_request_data( $c );
    throws_error($self, $c, $create_data);

    ## at the moment response data is the number of created new records or { error => { status  => ...,
    #											message => ...}}
    my $response_data = process_POST_request( $c, $create_data );
    throws_error($self, $c, $response_data);

    $c->stash->{ response_data } = $response_data;
    $self->status_ok(
                        $c,
                        entity => $response_data,
                    );
}

=head2 api_list_DELETE

=cut

sub api_list_DELETE {
    my ( $self, $c ) = @_;

    my $delete_data = process_PUT_POST_DELETE_request_data( $c );
    throws_error($self, $c, $delete_data );

    ## at the moment response data is the number of created new records or { error => { status  => ...,
    #											message => ...}}
    my $response_data = process_DELETE_request( $c, $delete_data );
    throws_error($self, $c, $response_data);

    $c->stash->{ response_data } = $response_data;
    $self->status_ok(
                        $c,
                        entity => $response_data,
                    );
}

__PACKAGE__->meta->make_immutable;

1;
