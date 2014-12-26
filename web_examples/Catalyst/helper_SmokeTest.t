use strict;
use warnings;
use Test::More;
use Test::Deep;

use Test::MockObject::Extends;
use Test::MockObject;
use Test::MockModule;

use LWP::UserAgent;
use YAML::Tiny qw(LoadFile);

use FindBin qw($Bin);

use lib ("$Bin/../lib");

=head2 Set up the testing environment

Running the ASSETConfig initConfig method replaces the dev/live database with a testing one. 

=cut

my $config_info;

my ($dsn, $dbh, $sql, $sth);
my ($filename, $fhandle);

BEGIN { use ASSETConfig qw( initConfig returnConfig );
	initConfig('/opt/Assetdb/etc/config_test.ini');
	$config_info = returnConfig();

	my $session_file = $config_info->{ ASSET }->{ session_file };
	unlink $session_file if -e $session_file;
}

BEGIN { 
	my @imports = qw(
			testConnection smoke 
			);
	use_ok ('SmokeTest', @imports); 
	can_ok(__PACKAGE__, 'testConnection');
	can_ok(__PACKAGE__, 'smoke');

	use_ok 'DBI';
	# connect to test database
	my $db      = $config_info->{ ASSET }->{ db };
	my $user    = $config_info->{ ASSET }->{ user };
	my $connect = $config_info->{ ASSET }->{ connect };

	$dsn = "dbi:mysql:$db:localhost";
	$dbh    = DBI->connect($dsn, $user, $connect) or die "Unable to connect: $DBI::errstr\n";


	## we need to fill in certain tables with correct data
	open(my $fh, '<', "$Bin/data4assetdb_test_tables.sql") or die $!;
	while ( <$fh> ) {
		chomp $_;
		$dbh->do( $_ ) or die $DBI::errstr;
	}

}


=head2 Mocking

we need to mock the LWP::UserAgent module so that we do not send real requests to ZenOSS when testing ZenossAPI methods

=cut

my ($url,  $ua, $mock_ua, $response);

$ua = LWP::UserAgent->new();
$mock_ua = Test::MockObject::Extends->new($ua);

=head2 Testing private methods

	_cleanUp
	_checkOK
	_readIUrls
	_makeRequest

=cut

diag("Testing private methods");
diag("\t_cleanUp\n\t_readInUrls\n\t_checkOK\n\t_makeRequest");

my ($category, $output, @output, $path, $file);
my ($expected, @expected);

$category = 'Random text<b>Reason:</b> Spyware';
$output   = SmokeTest::_cleanUp($category);
is($output, 'Spyware', "_cleanUp (\"$category\"): returns correct output");
$category = "Random text<b>Reason:</b> The category '";
$output   = SmokeTest::_cleanUp($category);
is($output, '', "_cleanUp (\"$category\"): returns correct output");
$category = "' has been blockedBLAHBLAH";
$output   = SmokeTest::_cleanUp($category);
is($output, '', "_cleanUp (\"$category\"): returns correct output");

$path = "$Bin";
$file = 'categories.cnf';
$expected = {
'http://gator.com/' => 'Spyware : gator.com',
'https://secure.toucan.org/toucan.com.txt' => 'TOUCAN-Test-File',
'http://httpmonitor:8085/' => 'healthcheck',
'http://httpmonitor:8085/check-virusengine/viruschecker' => 'healthcheck',
'http://www.newcastlebrown.com/' => 'Alcohol',
'http://www.artguide.org/' => 'Education',
'http://www.loot.co.uk/' => 'Search Engines / Directories / Portals',
'http://www.hsbc.co.uk/' => 'Banking',
};
@expected = qw(http://gator.com/ https://secure.toucan.org/toucan.com.txt http://httpmonitor:8085/ http://httpmonitor:8085/check-virusengine/viruschecker http://www.newcastlebrown.com/ http://www.artguide.org/ http://www.loot.co.uk/ http://www.hsbc.co.uk/);
@output   = SmokeTest::_readInUrls($path, $file);
is_deeply($output[0], $expected, "_readInUrls (\"$path\", \"$file\"): returns correct \$category_ref output");
is_deeply($output[1], \@expected, "_readInUrls (\"$path\", \"$file\"): returns correct \$urls_ref output");
$output   = SmokeTest::_readInUrls($path, $file);
$path = '';
$file = 'fakefile';
$output   = SmokeTest::_readInUrls($path, $file);
is($output, undef, "\t(\"$path\", \"$file\"): returns undef");
$path = '/fake/path';
$file = 'fakefile';
$output   = SmokeTest::_readInUrls($path, $file);
is($output, undef, "\t(\"$path\", \"$file\"): returns undef");

my $content = 'Education, Banking, Search Engines / Directories / Portals';
$url        = 'http://www.hsbc.co.uk/';
my $category_ref = $expected;
$expected        = join "\t", ( 0, $url, 'Education<br /> Banking<br /> Search Engines / Directories / Portals', 'Banking', 'OK' );
$output = SmokeTest::_checkOK($content, $url, $category_ref);
is_deeply($output, $expected, "_checkOK ($content, $url, \$category_ref): returns correct output");

$content = 'Education, Banking, Search Engines / Directories / Portals';
$url     = 'http://uncategorized.site/';
$expected = join "\t", ( 1, $url, 'Education<br /> Banking<br /> Search Engines / Directories / Portals', '', 'FAIL' );
$output = SmokeTest::_checkOK($content, $url, $category_ref);
is($output, $expected, "_checkOK ($content, $url, \$category_ref): returns correct output");


$mock_ua->mock( 'timeout', sub { shift; return 1; });
$mock_ua->mock( 'proxy',   sub { shift; return 1; });
$mock_ua->mock( 'get',     sub { shift; 
				 return { _content => 'Education, Banking, Search Engines / Directories / Portals' }; });
$expected = {_content => 'Education, Banking, Search Engines / Directories / Portals' };

$output = SmokeTest::getUrl($ua, 'hostname', 8080, 'http://site.url/');
is_deeply($output, $expected, "getUrl (\$ua, 'hostname', 8080, 'http://site.url/'): returns correct output");
$output = SmokeTest::getUrl('not a LWP::UserAgent object', 'hostname', 8080, 'http://site.url/');
is($output, undef, "\tgetUrl ('not a LWP::UserAgent object', 'hostname', 8080, 'http://site.url/'): returns undef");
$output = SmokeTest::getUrl($ua, '', 8080, 'http://site.url/');
is($output, undef, "\tgetUrl (\$ua, '', 8080, 'http://site.url/'): returns undef");
$output = SmokeTest::getUrl($ua, 'hostname', undef, 'http://site.url/');
is($output, undef, "\tgetUrl (\$ua, 'hostname', undef, 'http://site.url/'): returns undef");
$output = SmokeTest::getUrl($ua, 'hostname', 8080, '');
is($output, undef, "\tgetUrl (\$ua, 'hostname', 8080, ''): returns undef");

## For testing of public methods, we shall mock the private method 
my $mock_st = new Test::MockModule('SmokeTest');
$mock_st->mock('getUrl', sub ($$$$;$) { 
				shift; 
				my ($host, $proxyport, $url) = (shift; shift; shift);
				return { _content => 'Education, Banking, Search Engines / Directories / Portals' } 
					                                            if $host && $proxyport && $url; 
               }); 

$url          = 'http://www.hsbc.co.uk/';
my $host      = 'testhost';
my $proxyport =  9999;
$expected        = join "\t", ( 0, $url, 'Education<br /> Banking<br /> Search Engines / Directories / Portals', 'Banking', 'OK' );
$output = SmokeTest::_makeRequest($proxyport, $url, $host, $category_ref);
is($output, $expected, "_makeRequest ($proxyport, $url, $host, \$category_ref): returns correct output");
$output = SmokeTest::_makeRequest(undef, $url, $host, $category_ref);
is($output, undef, "_makeRequest (undef, $url, $host, \$category_ref): returns undef");
$output = SmokeTest::_makeRequest($proxyport, '', $host, $category_ref);
is($output, undef, "_makeRequest ($proxyport, '', $host, \$category_ref): returns undef");
$output = SmokeTest::_makeRequest($proxyport, $url, '', $category_ref);
is($output, undef, "_makeRequest ($proxyport, $url, '', \$category_ref): returns undef");

## _joinThreads
my @fake_threads = ();
{
	foreach ( 1 .. 4 ) {
		push @fake_threads, mockThread->new();
	}
	@expected = qw(1:AAA 1:AAA 1:AAA 1:AAA);
	my $tme   = time();
	@output   = SmokeTest::_joinThreads($tme, 25, \@fake_threads);
	is_deeply(\@output, \@expected, "_joinThreads ($tme, 25, \$threads_array_ref): returns correct output");
	$output   = SmokeTest::_joinThreads($tme, undef, \@fake_threads);
	is($output, undef, "_joinThreads ($tme, undef, \$threads_array_ref): returns undef");
	$output   = SmokeTest::_joinThreads(undef, 25, \@fake_threads);
	is($output, undef, "_joinThreads (undef, 25, \$threads_array_ref): returns undef");
}

=head2 Testing public methods

We are mocking the private methods _getURLBase and _getRequest

=cut

diag("Testing public methods");
diag("\ttestConnection\n\tsmoke");

## testConnection
##==========
$mock_st->mock('getUrl', sub ($$$$;$) { 
				shift; 
				my $host = shift; my $proxyport = shift; my $url = shift;
				return { _content => 'Any odd content as _checkOK will be mocked' } 
					if $host && $proxyport && $url; }); 
$content = 'mocked content';
$mock_st->mock('_checkOK', sub ($$$) { 
				shift, shift, shift; 
				return "$content";});
my ($cat_path, $cat_file) = ($config_info->{ SMOKETEST }->{ cat }, $config_info->{ SMOKETEST }->{ file });
@expected = (0, "$content");

$output = testConnection($proxyport, 
		    $cat_path, $cat_file, 
		    'testhost');
is($output, 1, "testConnection ($proxyport, $cat_path, $cat_file, 'testhost'), \n\t\toriginal content ... '$content': returns correct output: 1");

$mock_st->mock('_checkOK', sub ($$$) { 
 				shift, shift, shift; 
 				return ":$content";});
$output = testConnection($proxyport, 
		    $cat_path, $cat_file, 
		    'testhost');
is($output, 1, "\ttestConnection ($proxyport, $cat_path, $cat_file, 'testhost'), \n\t\toriginal content ... ':$content': returns correct output: 1");

$mock_st->mock('_checkOK', sub ($$$) { 
 				shift, shift, shift; 
 				return "0:$content";});
$output = testConnection($proxyport, 
		         $cat_path, $cat_file, 
		         'testhost');
is($output, 1, "\ttestConnection ($proxyport, $cat_path, $cat_file, 'testhost'), \n\t\toriginal content ... '0:$content': returns correct output: 1");

$mock_st->mock('_checkOK', sub ($$$) { 
 				shift, shift, shift; 
 				return "5:$content";});
$output = testConnection($proxyport, 
		         $cat_path, $cat_file, 
		         'testhost');
is($output, 1, "\ttestConnection ($proxyport, $cat_path, $cat_file, 'testhost'), \n\t\toriginal content ... '5:$content': returns correct output: 1");
 
diag("Mocking getUrl to return undef");
$mock_st->mock('getUrl', sub ($$$$;$) { return undef } ); 
$output = testConnection($proxyport, 
		         $cat_path, $cat_file, 
		         'testhost');
is($output, undef, "\ttestConnection ($proxyport, $cat_path, $cat_file, 'testhost'), \n\t\tcorrectly returns undef");
$mock_st->unmock('getUrl'); 

## smoke
##======
$mock_st->mock('_makeRequest',  sub ($$$$) { shift; shift; shift; shift; 
					     return '1:AAA'; });
$mock_st->mock('_createThread', sub ($$$$) { shift; 
					     return '1:AAA' });
$mock_st->mock('_joinThreads',  sub ($$$)  { shift; shift; shift;
					     return qw( 1:AAA 1:BBB 0:CCC 1:DDD ); });
my @results  = qw( AAA BBB CCC DDD );
$expected = join( "\n", @results );
@expected = (3, $expected);
@output   = smoke ($proxyport, 25, 'testhost', $cat_path, $cat_file);
is_deeply(\@output, \@expected, "smoke ($proxyport, 25, 'testhost', $cat_path, $cat_file): returns correct output: (0, '$content')");
$output   = smoke (undef, 25, 'testhost', $cat_path, $cat_file);
is($output, undef, "\tsmoke (undef, 25, 'testhost', $cat_path, $cat_file): returns undef");

$output   = smoke ($proxyport, undef, 'testhost', $cat_path, $cat_file);
is($output, undef, "\tsmoke ($proxyport, undef, 'testhost', $cat_path, $cat_file): returns undef");

$output   = smoke ($proxyport, 25, 'testhost', undef, $cat_file);
is($output, undef, "\tsmoke ($proxyport, 25, 'testhost', undef, $cat_file): returns undef");

$output   = smoke ($proxyport, 25, 'testhost', $cat_path, undef);
is($output, undef, "\tsmoke ($proxyport, 25, 'testhost', $cat_path, undef): returns undef");

## we want to simulate a timeout, so we shall mock the list method from the threads module
my $mock_threads = new Test::MockModule('threads');
$mock_threads->mock('list', sub { shift; return @fake_threads; });
@expected = ( 100, '', '', '', 'FAIL' );
@output   = smoke ($proxyport, 25, 'testhost', $cat_path, $cat_file);
is_deeply(\@output, \@expected, "\tsmoke ($proxyport, 25, 'testhost', $cat_path, $cat_file): returns fails correctly with  timeout: (100, '', '', '', 'FAIL')");


done_testing();

#-------------------------------------------------------------------------
package mockThread;

sub new {
	my $class = shift;
	
	my $self = {};
	return bless $self, $class;
}

sub is_joinable {
	my $self = shift;
	return 1;
}

sub join {
	my $self = shift;
	return '1:AAA';
}

sub detach {
	my $self = shift;
	return 1;
}


1;
#-------------------------------------------------------------------------
