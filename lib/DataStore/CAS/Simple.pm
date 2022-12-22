package DataStore::CAS::Simple;
use 5.008;
use Moo 1.000007;
use Carp;
use Try::Tiny;
use Digest 1.16 ();
use File::Spec 3.33;
use File::Spec::Functions 'catfile', 'catdir', 'canonpath';
use File::Temp 0.22 ();

our $VERSION = '0.07';
our @CARP_NOT= qw( DataStore::CAS DataStore::CAS::File DataStore::CAS::VirtualHandle );

# ABSTRACT: Simple file/directory based CAS implementation

=head1 DESCRIPTION

This implementation of L<DataStore::CAS> uses a directory tree where the
filenames are the hexadecimal value of the digest hashes.  The files are
placed into directories named with a prefix of the digest hash to prevent
too many entries in the same directory (which is actually only a concern
on certain filesystems).

Opening a L<File|DataStore::CAS::File> returns a real perl filehandle, and
copying a File object from one instance to another is optimized by hard-linking
the underlying file.

  # This is particularly fast:
  $cas1= DataStore::CAS::Simple->new( path => 'foo' );
  $cas2= DataStore::CAS::Simple->new( path => 'bar' );
  $cas1->put( $cas2->get( $hash ) );

This class does not perform any sort of optimization on the storage of the
content, neither by combining commom sections of files nor by running common
compression algorithms on the data.

TODO: write DataStore::CAS::Compressor or DataStore::CAS::Splitter
for those features.

=head1 ATTRIBUTES

=head2 path

Read-only.  The filesystem path where the store is rooted.

=head2 digest

Read-only.  Algorithm used to calculate the hash values.  This can only be
set in the constructor when a new store is being created.  Default is C<SHA-1>.

=head2 fanout

Read-only.  Returns arrayref of pattern used to split digest hashes into
directories.  Each digit represents a number of characters from the front
of the hash which then become a directory name.  The final digit may be
the character '=' to indicate the filename is the full hash, or '*' to
indicate the filename is the remaining digits of the hash.  '*' is the
default behavior if the C<fanout> does not include one of these characters.

For example, C<[ 2, 2 ]> would turn a hash of "1234567890" into a path of
"12/34/567890".  C<[ 2, 2, '=' ]> would turn a hash of "1234567890" into
a path of "12/34/1234567890".

=head2 fanout_list

Convenience accessor for C<< @{ $cas->fanout } >>

=head2 copy_buffer_size

Number of bytes to copy at a time when saving data from a filehandle to the
CAS.  This is a performance hint, and the default is usually fine.

=head2 storage_format_version

Hashref of version information about the modules that created the store.
Newer library versions can determine whether the storage is using an old
format using this information.

=cut

has path             => ( is => 'ro', required => 1 );
has copy_buffer_size => ( is => 'rw', default => sub { 256*1024 } );
has _config          => ( is => 'rwp', init_arg => undef );
sub fanout              { [ $_[0]->fanout_list ] }
sub fanout_list         { @{ $_[0]->_config->{fanout} } }
sub digest              { $_[0]->_config->{digest} }
has _digest_hash_to_hex => ( is => 'rw', init_arg => undef );
has _digest_hash_split  => ( is => 'rw', init_arg => undef );

with 'DataStore::CAS';

=head1 METHODS

=head2 new

  $class->new( \%params | %params )

Constructor.  It will load (and possibly create) a CAS Store.

If C<create> is specified, and C<path> refers to an empty directory, a fresh
store will be initialized.  If C<create> is specified and the directory is
already a valid CAS, C<create> is ignored, as well as C<digest> and
C<fanout>.

C<path> points to the cas directory.  Trailing slashes don't matter.
You might want to use an absolute path in case you C<chdir> later.

C<copy_buffer_size> initializes the respective attribute.

The C<digest> and C<fanout> attributes can only be initialized if
the store is being created.
Otherwise, it is loaded from the store's configuration.

C<ignore_version> allows you to load a Store even if it was created with a
newer version of the DataStore::CAS::Simple package that you are now using.
(or a different package entirely)

=for Pod::Coverage BUILD

=cut

sub BUILD {
	my ($self, $args)= @_;
	my ($create, $ignore_version, $digest, $fanout, $_notest)=
		delete @{$args}{'create','ignore_version','digest','fanout','_notest'};

	# Check for invalid params
	my @inval= grep { !$self->can($_) } keys %$args;
	croak "Invalid parameter: ".join(', ', @inval)
		if @inval;

	# Path is required, and must be a directory
	my $path= $self->path;
	if (!-d $path) {
		croak "Path '$path' is not a directory"
			unless $create;
		mkdir $path
			or die "Can't create directory '$path'";
	}	

	# Check directory
	my $setup= 0;
	unless (-f catfile($path, 'conf', 'VERSION')) {
		croak "Path does not appear to be a valid CAS : '$path'"
			unless $create;

		# Here, we are creating a new CAS directory
		$self->create_store({ digest => $digest, path => $path, fanout => $fanout });
		$setup= 1;
	}

	$self->_set__config( $self->_load_config($path, { ignore_version => $ignore_version }) );
	my ($tohex, $split)= _get_hex_and_fanout_functions($self->digest, $self->fanout);
	$self->_digest_hash_to_hex($tohex);
	$self->_digest_hash_split($split);

	if ($setup) {
		$self->put('');
	} else {
		# Properly initialized CAS will always contain an entry for the empty string
		croak "CAS dir '$path' is missing a required file"
		     ." (has it been initialized?)"
			unless $self->validate($self->hash_of_null);
	}

	return $self;
}

=head2 path_parts_for_hash

  my (@path)= $cas->path_parts_for_hash($digest_hash);

Given a hash string, return the directory parts and filename where that content
would be found.  They are returned as a list.  If the hash is not valid for this
digest algorithm, this will throw an exception.

=head2 path_for_hash

  my $path= $cas->path_for_hash($digest_hash);
  my $path= $cas->path_for_hash($digest_hash, $create_dirs);

Given a hash string, return the path to the file, including C<< $self->path >>.
The second argument can be set to true to create any missing directories in
this path.

=cut

sub path_parts_for_hash {
	my ($self, $hash)= @_;
	$self->_digest_hash_split->($hash);
}

sub path_for_hash {
	my ($self, $hash, $create_dirs)= @_;
	my @parts= $self->_digest_hash_split->($hash);
	if ($create_dirs) {
		my $path= $self->path;
		for (@parts[0..($#parts-1)]) {
			$path= catdir($path, $_);
			next if -d $path;
			mkdir($path) or croak "mkdir($path): $!";
		}
		return catfile($path, $parts[-1]);
	} else {
		return catfile($self->path, @parts);
	}
}

=head2 create_store

  $class->create_store( %configuration | \%configuration )

Create a new store at a specified path.  Configuration must include C<path>,
and may include C<digest> and C<fanout>.  C<path> must be an empty writeable
directory, and it must exist.  C<digest> currently defaults to C<SHA-1>.
C<fanout> currently defaults to C<[1, 2]>, resulting in paths like "a/bc/defg".

This method can be called on classes or instances.

You may also specify C<create =E<gt> 1> in the constructor to implicitly call
this method using the relevant parameters you supplied to the constructor.

=cut

sub create_store {
	my $class= shift;
	$class= ref $class if ref $class;
	my %params= (@_ == 1? %{$_[0]} : @_);
	
	defined $params{path} or croak "Missing required param 'path'";
	-d $params{path} or croak "Directory '$params{path}' does not exist";
	# Make sure we are creating in an empty dir
	croak "Directory '$params{path}' is not empty\n"
		unless $class->_is_dir_empty($params{path});

	$params{digest} ||= 'SHA-1';
	$class->_assert_digest_available($params{digest});

	$params{fanout} ||= [ 1, 2 ];
	# make sure the fanout isn't insane
	$params{fanout}= $class->_parse_fanout(join(' ',@{$params{fanout}}));

	my $conf_dir= catdir($params{path}, 'conf');
	mkdir($conf_dir) or croak "mkdir($conf_dir): $!";
	$class->_write_config_setting($params{path}, 'VERSION', $class->_hierarchy_version);
	$class->_write_config_setting($params{path}, 'digest', $params{digest}."\n");
	$class->_write_config_setting($params{path}, 'fanout', join(' ', @{$params{fanout}})."\n");
}
sub _hierarchy_version {
	my $class= ref $_[0] || $_[0];
	my $out= '';
	# record the version of any class hierarchy which "isa DataStore::CAS::Simple"
	require MRO::Compat if $] < 5.010;
	my $hier= mro::get_linear_isa($class);
	for (grep $_->isa(__PACKAGE__), @$hier) {
		if (!$_->VERSION) {
			warn "Package '$_' lacks a VERSION, weakening the protection of DataStore::CAS::Simple's versioned storage directory.";
		} else {
			$out .= $_ . ' ' . $_->VERSION . "\n";
		}
	}
	return $out;
}

# This method loads the digest and fanout configuration and validates it
# It is called during the constructor.
sub _load_config {
	my ($class, $path, $flags)= @_;
	$class= ref $class if ref $class;
	my %params;
	
	# Version str is "$PACKAGE $VERSION\n", where version is a number but might have a
	#   string suffix on it
	$params{storage_format_version}=
		$class->_parse_version($class->_read_config_setting($path, 'VERSION'));
	unless ($flags->{ignore_version}) {
		while (my ($pkg, $ver)= each %{$params{storage_format_version}}) {
			my $cur_ver= try { $pkg->VERSION };
			defined $cur_ver
				or croak "Class mismatch: storage dir was created using $pkg"
					." but that package is not loaded now\n";
			(try { $pkg->VERSION($ver); 1; } catch { 0 })
				or croak "Version mismatch: storage dir was created using"
					." version '$ver' of $pkg but this is only $cur_ver\n";
		}
	}

	# Get the digest algorithm name
	$params{digest}=
		$class->_parse_digest($class->_read_config_setting($path, 'digest'));
	$class->_assert_digest_available($params{digest});
	# Get the directory fan-out specification
	$params{fanout}= $class->_parse_fanout($class->_read_config_setting($path, 'fanout'));
	return \%params;
}

sub _get_hex_and_fanout_functions {
	my ($digest, $fanout)= @_;
	my $hexlen= length Digest->new($digest)->add('')->hexdigest;
	my $rawlen= length Digest->new($digest)->add('')->digest;
	# Create a function that coerces the argument into a hex string, or dies.
	# When given a digest, it can be raw bytes, or hex.  The  hex one is double the length.
	my $tohex= sub {
		my $hash= $_[2];
		my $len= length($hash) || 0;
		$len == $hexlen? $hash
		: $len == $rawlen? _to_hex($hash)
		: croak "Invalid length for checksum of $digest: $len "._quoted($hash);
	};

	# Create a function that splits a digest into the path components
	# for the CAS file.
	$fanout= [ @$fanout ];
	# final component might be a character indicating full-name or remainder-name
	my $filename_type= $fanout->[-1] =~ /^[0-9]+$/? '*'
		: pop @$fanout;
	my $re= '^'.join('', map "([0-9a-f]{$_})", map /([0-9]+)/, @$fanout);
	$re .= '([0-9a-f]+)' if $filename_type eq '*';
	$re = qr/$re/;
	my $split= ($filename_type eq '=')? sub {
			my $hash= $_[0];
			$hash= $tohex->($hash) if $hexlen != (length($hash) || 0);
			my @dirs= ($hash =~ $re) or croak "can't split hash '$hash' into requested fanout";
			return @dirs, $hash;
		}
		: ($filename_type eq '*')? sub {
			my $hash= $_[0];
			$hash= $tohex->($hash) if $hexlen != (length($hash) || 0);
			my @dirs= ($hash =~ $re) or croak "can't split hash '$hash' into requested fanout";
			return @dirs;
		}
		: croak "Unrecognized filename indicator in fanout specification: '$filename_type'";

	return ($tohex, $split);
}

sub _to_hex {
	my $tmp= shift;
	$tmp =~ s/./ sprintf("%02X", $_) /ge;
	$tmp;
}
sub _quoted {
	my $tmp= shift;
	return "(undef)" unless defined $tmp;
	$tmp =~ s/[\0-\x1F\x7F]/ sprintf("\\x%02X", $_) /ge;
	qq{"$tmp"};
}

sub _is_dir_empty {
	my (undef, $path)= @_;
	opendir(my $dh, $path)
		or die "opendir($path): $!";
	my @entries= grep { $_ ne '.' and $_ ne '..' } readdir($dh);
	closedir($dh);
	return @entries == 0;
}

# In the name of being "Simple", I decided to just read and write
# raw files for each parameter instead of using JSON or YAML.
# It is not expected that this module will have very many options.
# Subclasses will likely use YAML.

sub _write_config_setting {
	my (undef, $path, $name, $content)= @_;
	$path= catfile($path, 'conf', $name);
	open(my $f, '>', $path)
		or croak "Failed to open '$path' for writing: $!\n";
	(print $f $content) && (close $f)
		or croak "Failed while writing '$path': $!\n";
}
sub _read_config_setting {
	my (undef, $path, $name)= @_;
	$path= catfile($path, 'conf', $name);
	open(my $f, '<', $path)
		or croak "Failed to read '$path' : $!\n";
	local $/= undef;
	my $str= <$f>;
	defined $str and length $str or croak "Failed to read '$path' : $!\n";
	return $str;
}

# 4 hex digits makes 65536 subdirectories in a single parent
our $max_sane_level_fanout= 4;
# 6 hex digits creates 16 million directories, more than that is probably a mistake
our $max_sane_total_fanout= 6;
sub _parse_fanout {
	my (undef, $fanout)= @_;
	chomp($fanout);
	my @fanout= split /\s+/, $fanout;
	# Sanity check on the fanout
	my $total_digits= 0;
	for (@fanout) {
		if ($_ =~ /^(\d+)$/) {
			$total_digits+= $1;
			croak "Too large fanout in one directory ($1)" if $1 > $max_sane_level_fanout;
		} elsif ($_ eq '=' or $_ eq '*') {
			# indicates full hash for filename, or partial hash
			# must be the final element
			\$_ == \$fanout[-1] or croak "Fanout '+' or '=' can only be final element";
		} else {
			croak "Invalid character in fanout specification: '$_'";
		}
	}
	croak "Too many digits of fanout! ($total_digits)" if $total_digits > $max_sane_total_fanout;
	return \@fanout;
}

sub _parse_digest {
	my (undef, $digest)= @_;
	chomp($digest);
	($digest =~ /^(\S+)$/)
		or croak "Invalid digest algorithm name: '$digest'\n";
	return $1;
}

sub _parse_version {
	my (undef, $version)= @_;
	my %versions;
	for my $line (split /\r?\n/, $version) {
		($line =~ /^([A-Za-z0-9:_]+) ([0-9.]+)/)
			or croak "Invalid version string: '$line'\n";
		$versions{$1}= $2;
	}
	return \%versions;
}

=head2 get

See L<DataStore::CAS/get> for details.

=cut

sub get {
	my ($self, $hash)= @_;
	my $fname= $self->path_for_hash($hash);
	return undef
		unless (my ($size, $blksize)= (stat $fname)[7,11]);
	return bless {
		# required
		store      => $self,
		hash       => $hash,
		size       => $size,
		# extra info
		block_size => $blksize,
		local_file => $fname,
	}, 'DataStore::CAS::Simple::File';
}

=head2 put

See L<DataStore::CAS/put> for details.

=head2 put_scalar

See L<DataStore::CAS/put_scalar> for details.

=head2 put_file

See L<DataStore::CAS/put_file> for details. In particular, heed the warnings
about using the 'hardlink' and 'reuse_hash' flag.

DataStore::CAS::Simple has special support for the flags 'move' and 'hardlink'.
If your source is a real file on the same filesystem by the same owner and/or
group, C<< { move => 1 } >> will move the file instead of copying it.  (If it
is a different filesystem or ownership can't be changed, it gets copied and the
original gets unlinked).  If the file is a real file on the same filesystem with
correct owner and permissions, C<< { hardlink => 1 } >> will link the file into
the CAS instead of copying it.

=cut

sub put_file {
	my ($self, $file, $flags)= @_;
	my $ref= ref $file;
	my $is_cas_file= $ref && $ref->isa('DataStore::CAS::File');
	my $is_filename= DataStore::CAS::_thing_stringifies_to_filename($file);
	croak "Unhandled argument to put_file: ".($file||'(undef)')
		unless defined $file && ($is_cas_file || $is_filename);
	
	# Can only optimize if source is a real file
	if ($flags->{hardlink} || ($flags->{move} && !$is_cas_file)) {
		my $fname= $is_filename? "$file"
			: $is_cas_file && $file->can('local_file')? $file->local_file
			: undef;
		if ($fname && -f $fname) {
			my %known_hashes= $flags->{known_hashes}? %{$flags->{known_hashes}} : ();
			# Apply reuse_hash feature, if requested
			$known_hashes{$file->store->digest}= $file->hash
				if $is_cas_file && $flags->{reuse_hash};
			# Calculate the hash if it wasn't given.
			my $hash= ($known_hashes{$self->digest} ||= $self->calculate_file_hash($fname));
			# Have it already?
			if (-f $self->path_for_hash($hash)) {
				$flags->{stats}{dup_file_count}++
					if $flags->{stats};
				$self->_unlink_source_file($file, $flags)
					if $flags->{move};
				return $hash;
			}
			# Save hash for next step
			$flags= { %$flags, known_hashes => \%known_hashes };
			# Try the move or hardlink operation.  If it fails, it returns false,
			# and this falls through to the default implementation that copies the
			# file.
			return $hash if $flags->{move}
				? $self->_try_put_move($fname, $flags)
				: $self->_try_put_hardlink($fname, $flags);
		}
	}
	# Else use the default implementation which opens and reads the file.
	return DataStore::CAS::put_file($self, $file, $flags);
}

sub _try_put_move {
	my ($self, $file, $flags)= @_;
	my $hash= $flags->{known_hashes}{$self->digest}; # calculated above
	# Need to be on same filesystem for this to work.
	my $dest= $self->path_for_hash($hash,1);
	my $tmp= "$dest.tmp";
	return 0 unless rename($file, $tmp);
	if (ref $file && ref($file)->isa('File::Temp')) {
		# File::Temp creates a writable handle, and operates on the
		# file using 'fd___' functions, so it needs closed to be safe.
		$file->close;
	}
	# Need to be able to change ownership to current user and remove write bits.
	try {
		my ($mode, $uid, $gid)= (stat $tmp)[2,4,5]
			or die "stat($tmp): $!\n";
		if (!$flags->{dry_run}) {
			chown($>, $), $tmp) or die "chown($> $), $tmp): $!\n"
				if ($uid && $uid != $>) or ($gid && $gid != $) );
			chmod(0444, $tmp) or die "chmod(0444, $tmp): $!\n"
				if 0444 != ($mode & 0777);
			rename($tmp, $dest)
				or die "rename($tmp, $dest): $!\n";
		}
		# record that we added a new hash, if stats enabled.
		if ($flags->{stats}) {
			$flags->{stats}{new_file_count}++;
			push @{ $flags->{stats}{new_files} ||= [] }, $hash;
		}
		$hash;
	}
	catch {
		warn "Can't optimize CAS insertion with move: $_";
		unlink $tmp;
		0;
	};
}

sub _try_put_hardlink {
	my ($self, $file, $flags)= @_;
	my $hash= $flags->{known_hashes}{$self->digest}; # calculated above
	# Refuse to link a file that is writeable by anyone.
	my ($mode, $uid, $gid)= (stat $file)[2,4,5];
	defined $mode && !($mode & 0222)
		or return 0;
	# Refuse to link a file owned by anyone else other than root
	(!$uid || $uid == $>) and (!$gid || $gid == $))
		or return 0;
	# looks ok.
	my $dest= $self->path_for_hash($hash,1);
	$flags->{dry_run}
		or link($file, $dest)
		or return 0;
	# record that we added a new hash, if stats enabled.
	if ($flags->{stats}) {
		$flags->{stats}{new_file_count}++;
		push @{ $flags->{stats}{new_files} ||= [] }, $hash;
	}
	# it worked
	return $hash;
}

=head2 new_write_handle

See L<DataStore::CAS/new_write_handle> for details.

=cut

sub new_write_handle {
	my ($self, $flags)= @_;
	$flags ||= {};
	my $known_hash= $flags->{known_hashes} && $flags->{known_hashes}{$self->digest};
	$known_hash= undef unless defined $known_hash && length $known_hash;
	my $data= {
		wrote   => 0,
		dry_run => $flags->{dry_run},
		hash    => $known_hash,
		stats   => $flags->{stats},
	};
	
	$data->{dest_file}= File::Temp->new( TEMPLATE => 'temp-XXXXXXXX', DIR => $self->path )
		unless $data->{dry_run};
	
	$data->{digest}= $self->_new_digest
		unless defined $data->{hash};
	
	return DataStore::CAS::FileCreatorHandle->new($self, $data);
}

sub _handle_write {
	my ($self, $handle, $buffer, $count, $offset)= @_;
	my $data= $handle->_data;

	# Figure out count and offset, then either write or no-op (dry_run).
	$offset ||= 0;
	$count ||= length($buffer)-$offset;
	my $wrote= (defined $data->{dest_file})? syswrite( $data->{dest_file}, $buffer, $count, $offset||0 ) : $count;

	# digest only the bytes that we wrote
	if (defined $wrote and $wrote > 0) {
		local $!; # just in case
		$data->{wrote} += $wrote;
		$data->{digest}->add(substr($buffer, $offset, $wrote))
			if defined $data->{digest};
	}
	return $wrote;
}

sub _handle_seek {
	croak "Seek unsupported (for now)"
}

sub _handle_tell {
	my ($self, $handle)= @_;
	return $handle->_data->{wrote};
}

=head2 commit_write_handle

See L<DataStore::CAS/commit_write_handle> for details.

=cut

sub commit_write_handle {
	my ($self, $handle)= @_;
	my $data= $handle->_data;
	
	my $hash= defined $data->{hash}?
		$data->{hash}
		: $data->{digest}->hexdigest;
	
	my $temp_file= $data->{dest_file};
	if (defined $temp_file) {
		# Make sure all data committed
		close $temp_file
			or croak "while saving '$temp_file': $!";
	}
	
	return $self->_commit_file($temp_file, $hash, $data);
}

sub _commit_file {
	my ($self, $source_file, $hash, $flags)= @_;
	# Find the destination file name
	my $dest_name= $self->path_for_hash($hash);
	# Only if we don't have it yet...
	if (-f $dest_name) {
		if ($flags->{stats}) {
			$flags->{stats}{dup_file_count}++;
		}
	}
	else {
		# make it read-only
		chmod(0444, "$source_file") or croak "chmod(0444, $source_file): $!";
		
		# Rename it into place
		# Check for missing directories after the first failure,
		#   in the spirit of keeping the common case fast.
		$flags->{dry_run}
			or rename("$source_file", $dest_name)
			or ($self->path_for_hash($hash, 1) and rename($source_file, $dest_name))
			or croak "rename($source_file => $dest_name): $!";
		# record that we added a new hash, if stats enabled.
		if ($flags->{stats}) {
			$flags->{stats}{new_file_count}++;
			push @{ $flags->{stats}{new_files} ||= [] }, $hash;
		}
	}
	$hash;
}

=head2 validate

See L<DataStore::CAS/validate> for details.

=cut

sub validate {
	my ($self, $hash)= @_;

	my $path= $self->path_for_hash($hash);
	return undef unless -f $path;

	open (my $fh, "<:raw", $path)
		or return 0; # don't die.  Errors mean "not valid", even if it might be a permission issue
	my $hash2= try { $self->_new_digest->addfile($fh)->hexdigest } catch {''};
	return ($hash eq $hash2? 1 : 0);
}

=head2 open_file

See L<DataStore::CAS/open_file> for details.

=cut

sub open_file {
	my ($self, $file, $flags)= @_;
	my $mode= '<';
	$mode .= ':'.$flags->{layer} if ($flags && $flags->{layer});
	open my $fh, $mode, $file->local_file
		or croak "open: $!";
	return $fh;
}

=head2 iterator

See L<DataStore::CAS/iterator> for details.

=cut

sub _slurpdir {
	my ($path, $digits)= @_;
	opendir my $dh, $_[0] || die "opendir: $!";
	[ sort grep { length($_) eq $digits } readdir $dh ]
}
sub iterator {
	my ($self, $flags)= @_;
	$flags ||= {};
	my @length= ( $self->fanout_list, length($self->hash_of_null) );
	$length[-1] -= $_ for @length[0..($#length-1)];
	my $path= "".$self->path;
	my @dirstack= ( _slurpdir($path, $length[0]) );
	return sub {
		return undef unless @dirstack;
		while (1) {
			# back out of a directory hierarchy that we have finished
			while (!@{$dirstack[-1]}) {
				pop @dirstack; # back out of directory
				return undef unless @dirstack;
				shift @{$dirstack[-1]}; # remove directory name
			}
			# Build the name of the next file or directory
			my @parts= map { $_->[0] } @dirstack;
			my $fname= catfile( $path, @parts );
			# If a dir, descend into it
			if (-d $fname) {
				push @dirstack, _slurpdir($fname, $length[scalar @dirstack]);
			} else {
				shift @{$dirstack[-1]};
				# If a file at the correct depth, return it
				if ($#dirstack == $#length && -f $fname) {
					return join('', @parts);
				}
			}
		}
	};
}

=head2 delete

See L<DataStore::CAS/delete> for details.

=cut

sub delete {
	my ($self, $digest_hash, $flags)= @_;
	my $path= $self->path_for_hash($digest_hash);
	if (-f $path) {
		unlink $path || die "unlink: $!"
			unless $flags && $flags->{dry_run};
		$flags->{stats}{delete_count}++
			if $flags && $flags->{stats};
		return 1;
	} else {
		$flags->{stats}{delete_missing}++
			if $flags && $flags->{stats};
		return 0;
	}
}

# This can be called as class or instance method.
# When called as a class method, '$digest_name' is mandatory,
#   otherwise it is unneeded.
sub _new_digest {
	my ($self, $digest_name)= @_;
	Digest->new($digest_name || $self->digest);
}

sub _assert_digest_available {
	my ($class, $digest)= @_;
	try {
		$class->_new_digest($digest)
	}
	catch {
		s/^/# /mg;
		croak "Digest algorithm '$digest' is not available on this system.\n$_\n"
	};
	1;
}

package DataStore::CAS::Simple::File;
use strict;
use warnings;
use parent 'DataStore::CAS::File';

=head1 FILE OBJECTS

File objects returned by DataStore::CAS::Simple have two additional attributes:

=head2 local_file

The filename of the disk file within DataStore::CAS::Simple's path which holds
the requested data.

=head2 block_size

The block_size parameter from C<stat()>, which might be useful for accessing
the file efficiently.

=cut

sub local_file { $_[0]{local_file} }
sub block_size { $_[0]{block_size} }

1; # End of File::CAS::Store::Simple
