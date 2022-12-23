#!perl -T
use strict;
use warnings;
use Try::Tiny;
use File::Temp;
use Test::More;

plan skip_all => 'This test requires Catalyst::Controller::SimpleCAS::Store to be installed'
	unless eval 'require Catalyst::Controller::SimpleCAS::Store';

ok( (eval <<'MyCAS.pl' or note $@), 'Compose a Store' );
  package MyCAS;
  our $VERSION= '0.1';
  use Moo;
  extends 'DataStore::CAS::Simple'; # or whichever backend you want
  with 'DataStore::CAS::CatalystControllerSimpleCASAdapter';
  with 'Catalyst::Controller::SimpleCAS::Store';
  1;
MyCAS.pl

my $tmpdir= File::Temp->newdir;
my $cas= new_ok( 'MyCAS', [ path => $tmpdir, digest => 'SHA-256', create => 1 ] );

my $hash1= $cas->put_scalar('Test');
is( $cas->content_size($hash1), 4, 'content_size' );
is( $cas->fetch_content($hash1), 'Test', 'fetch_content' );

# Catalyst SimpleCAS hard-codes SHA-1, so make sure this method got overridden
is( $cas->calculate_checksum('Test'), $hash1, 'calculate_checksum' );


done_testing;
