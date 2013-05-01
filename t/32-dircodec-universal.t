#! /usr/bin/env perl -T
use strict;
use warnings;

use Test::More;
use Digest;
use Data::Dumper;

use_ok('DataStore::CAS::Virtual') || BAIL_OUT;
use_ok('DataStore::CAS::FS::Dir') || BAIL_OUT;
use_ok('DataStore::CAS::FS::DirCodec') || BAIL_OUT;
use_ok('DataStore::CAS::FS::DirCodec::Universal') || BAIL_OUT;

my $cas= DataStore::CAS::Virtual->new();

subtest empty_dir => sub {
	my $hash= DataStore::CAS::FS::DirCodec->store($cas, 'universal', [], {});
	my $file= $cas->get($hash);
	is( $file->data, qq|CAS_Dir 09 universal\n{"metadata":{},\n "entries":[\n]}|, 'encode' );
	isa_ok( my $decoded= DataStore::CAS::FS::DirCodec->load($file), 'DataStore::CAS::FS::Dir', 'decode' );
	is( $decoded->iterator->(), undef, 'zero entries' );
	
	done_testing;
};

subtest one_dirent => sub {
	my @entries= (
		{ type => 'file', name => 'test' }
	);
	my $hash= DataStore::CAS::FS::DirCodec->store($cas, 'universal', \@entries, {});
	my $file= $cas->get($hash);
	my $expected= qq|CAS_Dir 09 universal\n{"metadata":{},\n "entries":[\n{"name":"test","type":"file"}\n]}|;
	is( $file->data, $expected, 'encode' );

	isa_ok( my $dir= DataStore::CAS::FS::DirCodec->load($file), 'DataStore::CAS::FS::Dir', 'decode' );

	my $iter= $dir->iterator;
	for (@entries) {
		is_deeply( $iter->()->as_hash, $_, 'entry matches' );
	}
	is( $iter->(), undef, 'end of list' );
	done_testing;
};

subtest many_dirent => sub {
	my %metadata= (
		foo => 1,
		bar => 2,
		baz => 3
	);
	my @entries= (
		{ type => 'file',     name => 'a',       size => 10,    ref => '0000',   foo => 42, sdlfjskldf => 'sldfjhlsdkfjh' },
		{ type => 'pipe',     name => 'f',       size => 1,     ref => undef,    bar => 'xyz' },
		{ type => 'blockdev', name => 'd',       size => 10000, ref => '1234',   },
		{ type => 'file',     name => "\x{100}", size => 1,     ref => "\x{C4}\x{80}" },
		{ type => 'file',     name => "\x{FF}",  size => 1,     ref => "\x{FF}"  },
		{ type => 'file',     name => 'b',       size => 10,    ref => '1111',   1 => 2, 3 => 4, 5 => 6},
		{ type => 'chardev',  name => 'e',       size => 0,     ref => '4321',   },
		{ type => 'symlink',  name => 'c',       size => 10,    ref => 'fedcba', },
		{ type => 'socket',   name => 'g',       size => 1,     ref => undef,    },
	);
	my @expected= (
		{ type => 'file',     name => 'a',       size => 10,    ref => '0000',   foo => 42, sdlfjskldf => 'sldfjhlsdkfjh' },
		{ type => 'file',     name => 'b',       size => 10,    ref => '1111',   1 => 2, 3 => 4, 5 => 6},
		{ type => 'symlink',  name => 'c',       size => 10,    ref => 'fedcba', },
		{ type => 'blockdev', name => 'd',       size => 10000, ref => '1234',   },
		{ type => 'chardev',  name => 'e',       size => 0,     ref => '4321',   },
		{ type => 'pipe',     name => 'f',       size => 1,     ref => undef,    bar => 'xyz' },
		{ type => 'socket',   name => 'g',       size => 1,     ref => undef,    },
		{ type => 'file',     name => "\x{FF}",  size => 1,     ref => "\x{FF}"  },
		{ type => 'file',     name => "\x{100}", size => 1,     ref => "\x{C4}\x{80}", },
	);

	ok( my $hash= DataStore::CAS::FS::DirCodec->store($cas, 'universal', \@entries, {}), 'encode' );
	my $file= $cas->get($hash);

	isa_ok( my $dir= DataStore::CAS::FS::DirCodec->load($file), 'DataStore::CAS::FS::Dir', 'decode' );

	my $iter= $dir->iterator;
	for (@expected) {
		is_deeply( $iter->()->as_hash, $_, 'entry matches' );
	}
	is( $iter->(), undef, 'and next returns undef' );
	done_testing;
};

subtest unicode => sub {
	my @entries= (
		{ type => 'file', name => "\xC4\x80\xC5\x90", size => '100000000000000000000000000', ref => '0000' },
		{ type => 'file', name => DataStore::CAS::FS::NonUnicode->new("\x80"), size => '1', ref => "\x{C4}\x{80}" },
	);
	my @expected= (
		{ type => 'file', name => DataStore::CAS::FS::NonUnicode->new("\x80"), size => '1', ref => "\x{C4}\x{80}" },
		{ type => 'file', name => "\xC4\x80\xC5\x90", size => '100000000000000000000000000', ref => '0000' },
	);
	my %metadata= (
		"\x{AC00}" => "\x{0C80}"
	);
	my $expected_serialized= qq|CAS_Dir 09 universal\n|
		.qq|{"metadata":{"\xEA\xB0\x80":"\xE0\xB2\x80"},\n|
		.qq| "entries":[\n|
		.qq|{"name":{"*NonUnicode*":"\xC2\x80"},"ref":"\xC3\x84\xC2\x80","size":"1","type":"file"},\n|
		.qq|{"name":"\xC3\x84\xC2\x80\xC3\x85\xC2\x90","ref":"0000","size":"100000000000000000000000000","type":"file"}\n|
		.qq|]}|;
	my $encoded= DataStore::CAS::FS::DirCodec::Universal->encode(\@entries, \%metadata);
	ok( !utf8::is_utf8($encoded), 'encoded as bytes' );
	is( $encoded, $expected_serialized, 'encoded correctly' );
	
	isa_ok( my $dir= DataStore::CAS::FS::DirCodec::Universal->decode({ file => 0, data => $encoded }), 'DataStore::CAS::FS::Dir' );
	is_deeply( $dir->metadata, \%metadata, 'deserialized metadata are correct' )
		or diag Dumper($dir->metadata);
	is_deeply( [ map { $_->as_hash } @{$dir->{_entries}} ], \@expected, 'deserialized entries are correct' )
		or diag Dumper($dir->{_entries});
	is( ref $dir->{_entries}[0]->name, 'DataStore::CAS::FS::NonUnicode' );
	done_testing;
};

done_testing;