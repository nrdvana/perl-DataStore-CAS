#! /usr/bin/env perl -T

use Test::More;

use_ok $_ or BAIL_OUT("use $_")
	for qw(
		DataStore::CAS
		DataStore::CAS::Virtual
		DataStore::CAS::Simple
	);

diag( "Testing DataStore::CAS $DataStore::CAS::VERSION, Perl $], $^X" );
done_testing;