#! /usr/bin/env perl -T

use Test::More;

diag "Testing on Perl $], $^X";
use_ok( $_ )? diag("  $_ version " . $_->VERSION) : BAIL_OUT("use $_")
	for qw(
		DataStore::CAS
		DataStore::CAS::Virtual
		DataStore::CAS::Simple
	);

done_testing;