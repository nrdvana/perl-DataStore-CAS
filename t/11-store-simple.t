#!perl -T

for (qw( DataStore::CAS DataStore::CAS::Simple DataStore::CAS::File Test::Builder Test::More )) {
	push @{$_.'::CARP_NOT'}, qw( DataStore::CAS DataStore::CAS::Simple DataStore::CAS::File Test::Builder Test::More );
}
use strict;
use warnings;
use Test::More;
use Try::Tiny;
use Data::Dumper;
use IO::File;
use File::stat;
use File::Spec::Functions 'catfile','catdir';
use File::Path 'remove_tree';
use Digest::SHA;

sub slurp {
	my $f= shift;
	if (ref $f ne 'GLOB') {
		open(my $handle, '<:raw', $f) or do { diag "open($f): $!"; return undef; };
		$f= $handle;
	}
	local $/= undef;
	my $x= <$f>;
	return $x;
}
sub writefile {
	my ($name, $str)= @_;
	open my $handle, '>', $name;
	print $handle $str or die "$!";
	close $handle or die "$!";
}
sub dies(&$) {
	my ($code, $comment)= @_;
	try {
		&$code;
		fail "Failed to die during '$comment'";
	}
	catch {
		ok "died - $comment";
	};
}
sub dies_like(&$$) {
	my ($code, $pattern, $comment)= @_;
	try {
		&$code;
		fail "Failed to die during '$comment'";
	}
	catch {
		like($_, $pattern, $comment);
	};
}

use_ok('DataStore::CAS::Simple') || BAIL_OUT;

chdir('t') if -d 't';
-d 'cas_tmp' or BAIL_OUT('missing cas_tmp directory for testing file-based cas');

my $casdir= catdir('cas_tmp','cas_store_simple');
my $casdir2= catdir('cas_tmp','cas_store_simple2');
my $casdir3= catdir('cas_tmp','cas_store_simple3');

subtest test_constructor => sub {
	remove_tree($casdir);
	mkdir($casdir) or die "$!";

	my $cas= new_ok('DataStore::CAS::Simple', [ path => $casdir, create => 1, digest => 'SHA-1', fanout => [2] ]);

	my $nullfile= catfile($casdir,'da','39a3ee5e6b4b0d3255bfef95601890afd80709');
	is( slurp($nullfile), '', 'null hash exists and is empty' );
	like( slurp(catfile($casdir,'conf','fanout')), qr/^2\r?\n$/, 'fanout file correctly written' );
	like( slurp(catfile($casdir,'conf','digest')), qr/^SHA-1\r?\n$/, 'digest file correctly written' );

	unlink $nullfile or die "$!";
	dies_like { DataStore::CAS::Simple->new(path => $casdir) } qr/missing a required/, 'missing null file';

	IO::File->new($nullfile, "w")->print("\n");
	dies_like { DataStore::CAS::Simple->new(path => $casdir) } qr/missing a required/, 'invalid null file';

	unlink $nullfile;
	unlink catfile($casdir,'conf','VERSION') or die "$!";
	dies_like { DataStore::CAS::Simple->new(path => $casdir) } qr/valid CAS/, 'invalid CAS dir';
	dies_like { DataStore::CAS::Simple->new(path => $casdir, create => 1) } qr/not empty/, 'can\'t create if not empty';

	remove_tree($casdir);
	mkdir($casdir) or die "$!";
	dies_like { DataStore::CAS::Simple->new(path => $casdir, create => 1, fanout => [6]) } qr/fanout/, 'fanout too wide';

	remove_tree($casdir);
	mkdir($casdir) or die "$!";
	dies_like { DataStore::CAS::Simple->new(path => $casdir, create => 1, fanout => [1,1,1,1,1,1,1]) } qr/fanout/, 'fanout too wide';

	$cas= new_ok('DataStore::CAS::Simple', [ path => $casdir, create => 1, digest => 'SHA-1', fanout => [1,1,1,1,1] ], 'create with deep fanout');
	$cas= undef;
	$cas= new_ok('DataStore::CAS::Simple', [ path => $casdir ], 're-open');
	done_testing;
};

subtest test_get_put => sub {
	remove_tree($casdir);
	mkdir($casdir) or die "$!";

	my $cas= new_ok('DataStore::CAS::Simple', [ path => $casdir, create => 1, digest => 'SHA-1' ]);

	isa_ok( (my $file= $cas->get( 'da39a3ee5e6b4b0d3255bfef95601890afd80709' )), 'DataStore::CAS::File', 'get null file' );
	is( $file->size, 0, 'size of null is 0' );

	is( $cas->get( '0'x40 ), undef, 'non-existent hash' );

	is( $cas->put(''), 'da39a3ee5e6b4b0d3255bfef95601890afd80709', 'put empty file again' );

	my $str= 'String of Text';
	my $hash= '00de5a1e6cc9c22ce07401b63f7b422c999d66e6';
	is( $cas->put($str), $hash, 'put scalar' );
	is( $cas->get($hash)->size, length($str), 'file length matches' );
	is( slurp($cas->get($hash)->open), $str, 'scalar read back correctly' );
	
	my $handle;
	open($handle, "<", \$str) or die;
	is( $cas->put($handle), $hash, 'put handle' );
	
	my $tmpfile= catfile('cas_tmp','test_file_1');
	writefile($tmpfile, $str);
	
	is( $cas->put_file($tmpfile), $hash, 'put_file(filename)' );
	is( $cas->put($cas->get($hash)), $hash, 'put DataStore::CAS::File' );
	
	SKIP: {
		skip 2, "Path::Tiny unavailable"
			unless eval 'require Path::Tiny;';
		is( $cas->put(Path::Tiny::path($tmpfile)), $hash, 'put(Path::Tiny)' );
		is( $cas->put_file(Path::Tiny::path($tmpfile)), $hash, 'put_file(Path::Tiny)' );
	}
	SKIP: {
		skip 1, "Path::Class unavailable"
			unless eval 'require Path::Class;';
		is( $cas->put(Path::Class::file($tmpfile)), $hash, 'put(Path::Class)' );
		is( $cas->put_file(Path::Class::file($tmpfile)), $hash, 'put_file(Path::Class)' );
	}
	my $ft= File::Temp->new();
	$ft->print($str);
	$ft->seek(0,0);
	is( $cas->put($ft), $hash, 'put(File::Temp)' );
	is( $cas->put_file($ft), $hash, 'put_file(File::Temp)' );
	
	done_testing;
};

subtest test_hardlink_optimization => sub {
	my $f1= catfile('cas_tmp', 'linktest.1');
	my $f2= catfile('cas_tmp', 'linktest.2');
	writefile($f1, '');
	unlink $f2;
	my $can_link= try { link($f1, $f2) or diag "link: $!" };
	my $can_cmp_inode= stat($f1)->ino == stat($f2)->ino;

	plan skip_all => 'hardlinks aren\'t available'
		unless $can_link;

	remove_tree($casdir);
	remove_tree($casdir2);
	remove_tree($casdir3);
	mkdir($casdir) or die "$!";
	mkdir($casdir2) or die "$!";
	mkdir($casdir3) or die "$!";

	my $cas1= new_ok('DataStore::CAS::Simple', [ path => $casdir,  create => 1, digest => 'SHA-1' ]);
	my $cas2= new_ok('DataStore::CAS::Simple', [ path => $casdir2, create => 1, digest => 'SHA-1' ]);
	my $cas3= new_ok('DataStore::CAS::Simple', [ path => $casdir3, create => 1, digest => 'SHA-256' ]);

	my $str= 'Testing Testing Testing';
	my $hash1= '36803d17c40ace10c936ab493d7a957c60bdce4a';
	my $hash256= 'e6ec36e4c3abf21935f8555c5f2c9ce755d67858291408ec02328140ae1ac8b0';

	is( $cas1->put($str, { reuse_hash => 1, hardlink => 1 }), $hash1, 'correct sha-1 hash' );
	my $file= $cas1->get($hash1) or die;
	is( $file->local_file, $cas1->path_for_hash($hash1), 'path is what we expected' );

	is( $cas2->put_file($file, { reuse_hash => 1, hardlink => 1 }), $hash1, 'correct sha-1 when migrated' );
	my $file2= $cas2->get($hash1) or die;
	is( $file2->local_file, $cas2->path_for_hash($hash1) );

	my $stat1= stat( $file->local_file ) or die "stat: $!";
	my $stat2= stat( $file2->local_file ) or die "stat: $!";
	is( $stat1->dev.','.$stat1->ino, $stat2->dev.','.$stat2->ino, 'inodes match - hardlink succeeded' )
		if $can_cmp_inode;

	# make sure it doesn't get the same hash when copied to a cas with different digest
	is( $cas3->put_file($file, { reuse_hash => 1, hardlink => 1 }), $hash256, 'correct sha-256 hash from sha-1 file' );
	my $file3= $cas3->get($hash256);
	my $stat3= stat( $file3->local_file ) or die "stat: $!";
	is( $stat3->dev.','.$stat3->ino, $stat1->dev.','.$stat1->ino, 'inodes match - hardlink succeeded' )
		if $can_cmp_inode;

	is( $cas1->put_file($file3, { reuse_hash => 1, hardlink => 1 }), $hash1, 'correct sha-1 hash from sha-2 file' );

	done_testing;
};

subtest test_move_optimization => sub {
	my $f1= catfile('cas_tmp', 'movetest.1');
	my $f2= catfile('cas_tmp', 'movetest.2');
	writefile($f1, '');
	unlink $f2;
	my $f1_ino= stat($f1)->ino;
	rename($f1, $f2);
	my $can_cmp_inode= $f1_ino == stat($f2)->ino;

	remove_tree($casdir);
	remove_tree($casdir2);
	mkdir($casdir) or die "$!";
	mkdir($casdir2) or die "$!";
	
	my $cas1= new_ok('DataStore::CAS::Simple', [ path => $casdir,  create => 1, digest => 'SHA-1' ]);
	my $cas2= new_ok('DataStore::CAS::Simple', [ path => $casdir2, create => 1, digest => 'SHA-1' ]);

	my $str= 'Testing Testing Testing';
	my $hash1= '36803d17c40ace10c936ab493d7a957c60bdce4a';
	my $hash256= 'e6ec36e4c3abf21935f8555c5f2c9ce755d67858291408ec02328140ae1ac8b0';

	# Simple test of insert+move
	writefile($f1, $str);
	$f1_ino= stat($f1)->ino;
	is( $cas1->put_file($f1, { move => 1 }), $hash1, 'correct sha-1 hash' );
	ok( !-f $f1, 'source no longer exists' );
	is( stat($cas1->get($hash1)->local_file)->ino, $f1_ino, 'same inode, move successful' );

	# Ensure CAS files don't get moved
	dies_like { $cas2->put($cas1->get($hash1), { move => 1 }) } qr/delete/, 'prevent deleting from CAS';
	ok( $cas1->get($hash1), 'cas1 still has the file' );

	done_testing;
};

subtest test_iterator => sub {
	remove_tree($casdir);
	mkdir($casdir) or die "$!";
	
	my $cas1= new_ok('DataStore::CAS::Simple', [ path => $casdir,  create => 1, digest => 'SHA-1' ]);
	isa_ok( my $i= $cas1->iterator, 'CODE' );
	is( $i->(), $cas1->hash_of_null, 'one element' );
	is( $i->(), undef, 'end of list' );

	my $hashes= {
		'String of Text' => '00de5a1e6cc9c22ce07401b63f7b422c999d66e6',
		'Testing'        => '0820b32b206b7352858e8903a838ed14319acdfd',
		'Something'      => 'b74dd130fe4e46c52aeb39878480cfe50324dab9',
		'Something1'     => 'ee6c06282ef9600df99eee106fb770b8c3dd1ff1',
		'Something2'     => 'ca2a1a4e26b79949243d23e526936bccca0493ce',
	};
	is( $cas1->put($_), $hashes->{$_} )
		for keys %$hashes;
	my @expected= sort (values %$hashes, $cas1->hash_of_null);
	$i= $cas1->iterator;
	my @actual;
	while (defined (my $x= $i->())) { push @actual, $x; }
	is_deeply( \@actual, \@expected, 'iterated correctly' );
	
	ok( $cas1->delete(delete $hashes->{'Testing'}), 'deleted item' );
	@expected= sort (values %$hashes, $cas1->hash_of_null);
	$i= $cas1->iterator;
	@actual= ();
	while (defined (my $x= $i->())) { push @actual, $x; }
	is_deeply( \@actual, \@expected, 'iterated correctly' );
	
	done_testing;
};

done_testing;
