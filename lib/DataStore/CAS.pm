package DataStore::CAS;
use 5.008;
use Moo::Role;
use Carp;
use Try::Tiny;
require Scalar::Util;
require Symbol;

our $VERSION= '0.03';
our @CARP_NOT= qw( DataStore::CAS::File DataStore::CAS::VirtualHandle );

# ABSTRACT: Abstract base class for Content Addressable Storage

=head1 DESCRIPTION

This module lays out a very straightforward API for Content Addressable
Storage.

Content Addressable Storage is a concept where a file is identified by a
one-way message digest checksum of its content.  (usually called a "hash")
With a good message digest algorithm, one checksum will statistically only
ever refer to one file, even though the permutations of the checksum are
tiny compared to all the permutations of bytes that they can represent.

Perl uses the term 'hash' to refer to a mapping of key/value pairs, which
creates a little confusion.  The documentation of this and related modules
try to use the phrase "digest hash" to clarify when we are referring to the
output of a digest function vs. a perl key-value mapping.

In short, a CAS is a key/value mapping where small-ish keys are determined
from large-ish data but no two pieces of data will ever end up with the same
key, thanks to astronomical probabilities.  You can then use the small-ish
key as a reference to the large chunk of data, as a sort of compression
technique.

=head1 PURPOSE

One great use for CAS is finding and merging duplicated content.  If you
take two identical files (which you didn't know were identical) and put them
both into a CAS, you will get back the same hash, telling you that they are
the same.  Also, the file will only be stored once, saving disk space.

Another great use for CAS is the ability for remote systems to compare an
inventory of files and see which ones are absent on the other system.
This has applications in backups and content distribution.

=head1 SYNOPSIS

  # Create a new CAS which stores everything in plain files.
  my $cas= DataStore::CAS::Simple->new(
    path   => './foo/bar',
    create => 1,
    digest => 'SHA-256',
  );
  
  # Store content, and get its hash code
  my $hash= $cas->put_scalar("Blah");
  
  # Retrieve a reference to that content
  my $file= $cas->get($hash);
  
  # Inspect the file's attributes
  $file->size < 1024*1024 or die "Use a smaller file";
  
  # Open a handle to that file (possibly returning a virtual file handle)
  my $handle= $file->open;
  my @lines= <$handle>;

=head1 ATTRIBUTES

=head2 digest

Read-only.  The name of the digest algorithm being used.

Subclasses must set this during their constructor.

The algorithm should be available from the L<Digest> module, or else the
subclass will need to provide a few additional methods like L</calculate_hash>.

=head2 hash_of_null

The digest hash of the empty string.

=cut

requires 'digest';

has hash_of_null => ( is => 'lazy' );

sub _build_hash_of_null {
	return shift->calculate_hash('');
}

=head1 METHODS

=head2 get

  $cas->get( $digest_hash )

Returns a L<DataStore::CAS::File> object for the given hash, if the hash
exists in storage. Else, returns undef.

This method is pure-virtual and must be implemented in the subclass.

=cut

requires 'get';

=head2 put

  $cas->put( $thing, \%optional_flags )

Convenience method.
Inspects $thing and passes it off to a more specific method.  If you want
more control over which method is called, call it directly.

=over 2

=item *

Scalars and Scalar-refs are passed to L</put_scalar>.

=item *

Instances of L<DataStore::CAS::File> or L<Path::Class::File> are passed to L</put_file>.

=item *

Globrefs or instances of L<IO::Handle> are passed to L</put_handle>.

=item *

Dies if it encounters anything else.

=back

The C<%optional_flags> can contain a wide variety of parameters, but
these are supported by all CAS subclasses:

=over

=item dry_run => $bool

Setting "dry_run" to true will calculate the hash of the $thing, and go through
the motions of writing it, but not store it.

=item known_hashes => \%digest_hashes

  { known_hashes => { SHA1 => '0123456789...' } }

Use this to skip calculation of the hash.  The hashes are keyed by Digest name,
so it is safe to use even when the store being written to might not use the same
digest that was already calculated.

Of course, using this feature can corrupt your CAS if you don't ensure that the
hash is correct.

=item stats => \%stats_out

Setting "stats" to a hashref will instruct the CAS implementation to return
information about the operation, such as number of bytes written, compression
strategies used, etc.  The statistics are returned within that supplied
hashref.  Values in the hashref are amended or added to, so you may use the
same stats hashref for multiple calls and then see the summary for all
operations when you are done.

=back

The return value is the hash checksum of the stored data, regardless of whether
it was already present in the CAS.

Example:

  my $stats= {};
  $cas->put("abcdef", { stats => $stats });
  $cas->put(\$large_buffer, { stats => $stats });
  $cas->put(IO::File->new('~/file','r'), { stats => $stats });
  $cas->put(\*STDIN, { stats => $stats });
  $cas->put(Path::Class::file('~/file'), { stats => $stats });
  use Data::Printer;
  p $stats;

=cut

sub put {
	my $ref= ref $_[1];
	goto $_[0]->can('put_scalar')
		if !$ref || $ref eq 'SCALAR';
	goto $_[0]->can('put_file')
		if $ref->isa('DataStore::CAS::File')
		or $ref->isa('Path::Class::File')
		or $ref->isa('Path::Tiny')
		or $ref->isa('File::Temp');
	goto $_[0]->can('put_handle')
		if $ref->isa('IO::Handle')
		or Scalar::Util::reftype($_[1]) eq 'GLOB';
	croak("Can't 'put' object of type $ref");
}

=head2 put_scalar

  $cas->put_scalar( $scalar, \%optional_flags )
  $cas->put_scalar( \$scalar, \%optional_flags )

Puts the literal string "$scalar" into the CAS, or the scalar pointed to by a
scalar-ref.  (a scalar-ref can help by avoiding a copy of a large scalar)
The scalar must be a string of bytes; you get an exception if any character
has a codepoint above 255.

Returns the digest hash of the array of bytes.

See L</put> for the discussion of C<%flags>.

=cut

sub put_scalar {
	my ($self, undef, $flags)= @_;
	my $ref= ref $_[1] eq 'SCALAR'? $_[1] : \$_[1];

	# Force to plain string if it is an object
	if (ref $$ref) {
		# TODO: croak unless object has stringify magic
		$ref= \"$$ref";
	}

	# Can only 'put' octets, not wide-character unicode strings.
	utf8::downgrade($$ref, 1)
		or croak "scalar must be byte string (octets).  If storing unicode,"
			." you must reduce to a byte encoding first.";

	my $hash= $flags && $flags->{known_hashes} && $flags->{known_hashes}{$self->digest}
		? $flags->{known_hashes}{$self->digest}
		: $self->calculate_hash($ref);
	if ($self->get($hash)) {
		# Already have it
		$flags->{stats}{dup_file_count}++
			if $flags->{stats};
		return $hash;
	} else {
		$flags= { ($flags? %$flags : ()), known_hashes => { $self->digest => $hash } };
		my $handle= $self->new_write_handle($flags);
		$handle->_write_all($$ref);
		return $self->commit_write_handle($handle);
	}
}

=head2 put_file

  $digest_hash= $cas->put_file( $filename, \%optional_flags );
  $digest_hash= $cas->put_file( $Path_Class_File, \%optional_flags );
  $digest_hash= $cas->put_file( $DataStore_CAS_File, \%optional_flags );

Insert a file from the filesystem, or from another CAS instance.
Default implementation simply opens the named file, and passes it to
put_handle.

Returns the digest hash of the data stored.

See L</put> for the discussion of standard C<%flags>.

Additional flags:

=over

=item move => $bool

If move is true, and the CAS is backed by plain files on the same filesystem,
it will move the file into the CAS, possibly changing its owner and permissions.
Even if the file can't be moved, C<put_file> will attempt to unlink it, and die
on failure.

=item hardlink => $bool

If hardlink is true, and the CAS is backed by plain files on the same filesystem
by the same owner and permissions as the destination CAS, it will hardlink the
file directly into the CAS.

This reduces the integrity of your CAS; use with care.  You can use the
L</validate> method later to check for corruption.

=item reuse_hash => $bool

This is a shortcut for known_hashes if you specify an instance of
L<DataStore::CAS::File>.  It builds a C<known_hashes> of one item using the
source CAS's digest algorithm.

=back

Note: A good use of these flags is to transfer files from one instance of
L<DataStore::CAS::Simple> to another.

  my $file= $cas1->get($hash);
  $cas2->put($file, { hardlink => 1, reuse_hash => 1 });

=cut

sub put_file {
	my ($self, $file, $flags)= @_;
	my $ref= ref $file;
	my $is_cas_file= $ref && $ref->isa('DataStore::CAS::File');
	my $is_filename= !$ref || $ref->isa('Path::Class::File') || $ref->isa('Path::Tiny')
		|| $ref->isa('File::Temp');
	croak "Unhandled argument to put_file: ".($file||'(undef)')
		unless defined $file && ($is_cas_file || $is_filename);

	my %known_hashes= $flags->{known_hashes}? %{$flags->{known_hashes}} : ();
	# Apply reuse_hash feature, if requested
	if ($is_cas_file && $flags->{reuse_hash}) {
		$known_hashes{$file->store->digest}= $file->hash;
		$flags= { %$flags, known_hashes => \%known_hashes };
	}
	# It is probably better to read a file twice than to write one that
	# doesn't need to be written.
	# ...but can't do better than ->put_handle unless the file is a real file.
	my $fname= $is_filename? "$file"
		: $is_cas_file && $file->can('local_file') && length $file->local_file? $file->local_file
		: undef;
	if (defined $fname && ($known_hashes{$self->digest} || -f $fname)) {
		# Calculate the hash if it wasn't given.
		my $hash= ($known_hashes{$self->digest} ||= $self->calculate_file_hash($fname));
		# Avoid unnecessary work
		if ($self->get($hash)) {
			$flags->{stats}{dup_file_count}++
				if $flags->{stats};
			$self->_unlink_source_file($file, $flags)
				if $flags->{move};
			return $hash;
		}
		# Save hash for next step
		$flags= { %$flags, known_hashes => \%known_hashes };
	}
	my $fh;
	if ($is_cas_file) {
		$fh= $file->open or croak "Can't open '$file': $!";
	}
	elsif ($ref && $ref->can('openr')) {
		$fh= $file->openr or croak "Can't open '$file': $!";
	}
	elsif ($is_filename) {
		open($fh, '<', $fname) or croak "Can't open '$fname': $!";
	}
	else {
		croak "Don't know how to open '$file'";
	}
	my $hash= $self->put_handle($fh, $flags);
	$self->_unlink_source_file($file, $flags)
		if $hash && $flags->{move};
	return $hash;
}

sub _unlink_source_file {
	my ($self, $file, $flags)= @_;
	return if $flags->{dry_run};
	my $is_cas_file= ref $file && ref($file)->isa('DataStore::CAS::File');
	if ($is_cas_file) {
		croak "Refusing to delete origin CAS File (this can damage a CAS)\n"
			."If you really want to do this, pass \$file->local_name and then"
			." delete the cas entry yourself.";
	} else {
		unlink "$file" or croak "unlink($file): $!"
	}
}

=head2 put_handle

  $digest_hash= $cas->put_handle( \*HANDLE | IO::Handle, \%optional_flags );

Reads from $io_handle and stores into the CAS.  Calculates the digest hash
of the data as it goes.  Does not seek on handle, so if you supply a handle
that is not at the start of the file, only the remainder of the file will be
added and hashed.  Dies on any I/O errors.

Returns the calculated hash when complete.

See L</put> for the discussion of C<flags>.

=cut

sub put_handle {
	my ($self, $h_in, $flags)= @_;
	binmode $h_in;
	my $h_out= $self->new_write_handle($flags);
	my $buf_size= $flags->{buffer_size} || 1024*1024;
	my $buf;
	while(1) {
		my $got= read($h_in, $buf, $buf_size);
		if ($got) {
			$h_out->_write_all($buf) or croak "write: $!";
		} elsif (!defined $got) {
			next if ($!{EINTR} || $!{EAGAIN});
			croak "read: $!";
		} else {
			last;
		}
	}
	return $self->commit_write_handle($h_out);
}

=head2 new_write_handle

  $handle= $cas->new_write_handle( %flags )

Get a new handle for writing to the Store.  The data written to this handle
will be saved to a temporary file as the digest hash is calculated.

When done writing, call either C<$cas->commit_write_handle( $handle )> (or the
alias C<$handle->commit()>) which returns the hash of all data written.  The
handle will no longer be valid.

If you free the handle without committing it, the data will not be added to
the CAS.

The optional 'flags' hashref can contain a wide variety of parameters, but
these are supported by all CAS subclasses:

=over

=item dry_run => $bool

Setting "dry_run" to true will calculate the hash of the $thing, but not store
it.

=item stats => \%stats_out

Setting "stats" to a hashref will instruct the CAS implementation to return
information about the operation, such as number of bytes written, compression
strategies used, etc.  The statistics are returned within that supplied
hashref.  Values in the hashref are amended or added to, so you may use the
same stats hashref for multiple calls and then see the summary for all
operations when you are done.

=back

Write handles will probably be an instance of L<FileCreatorHandle|DataStore::CAS::FS::FileCreatorHandle>.

=head2 commit_write_handle

  my $handle= $cas->new_write_handle();
  print $handle $data;
  $cas->commit_write_handle($handle);

This closes the given write-handle, and then finishes calculating its digest
hash, and then stores it into the CAS (unless the handle was created with the
dry_run flag).  It returns the digest_hash of the data.

=cut

# This implementation probably needs overridden by subclasses.
sub new_write_handle {
	my ($self, $flags)= @_;
	return DataStore::CAS::FileCreatorHandle->new($self, { flags => $flags });
}

# This must be implemented by subclasses
requires 'commit_write_handle';

=head2 calculate_hash

Return the hash of a scalar (or scalar ref) in memory.

=head2 calculate_file_hash

Return the hash of a file on disk.

=cut

sub calculate_hash {
	my $self= shift;
	Digest->new($self->digest)->add(ref $_[0]? ${$_[0]} : $_[0])->hexdigest;
}

sub calculate_file_hash {
	my ($self, $file)= @_;
	open my $fh, '<', $file or croak "open($file): $!";
	binmode $fh;
	Digest->new($self->digest)->addfile($fh)->hexdigest;
}

=head2 validate

  $bool_valid= $cas->validate( $digest_hash, \%optional_flags )

Validate an entry of the CAS.  This is used to detect whether the storage
has become corrupt.  Returns 1 if the hash checks out ok, and returns 0 if
it fails, and returns undef if the hash doesn't exist.

Like the L</put> method, you can pass a hashref in $flags{stats} which
will receive information about the file.  This can be used to implement
mark/sweep algorithms for cleaning out the CAS by asking the CAS for all
other digest_hashes referenced by $digest_hash.

The default implementation simply reads the file and re-calculates its hash,
which should be optimized by subclasses if possible.

=cut

sub validate {
	my ($self, $hash, $flags)= @_;

	my $file= $self->get($hash);
	return undef unless defined $file;

	# Exceptions during 'put' will most likely come from reading $file,
	# which means that validation fails, and we return false.
	my $new_hash;
	try {
		# We don't pass flags directly through to get/put, because flags for validate
		#  are not the same as flags for get or put.  But, 'stats' is a standard thing.
		my %args= ( dry_run => 1 );
		$args{stats}= $flags->{stats} if $flags->{stats};
		$new_hash= $self->put_handle($file, \%args);
	}
	catch {
	};
	return (defined $new_hash and $new_hash eq $hash)? 1 : 0;
}

=head2 delete

  $bool_happened= $cas->delete( $digest_hash, %optional_flags )

DO NOT USE THIS METHOD UNLESS YOU UNDERSTAND THE CONSEQUENCES

This method is supplied for completeness... however it is not appropriate
to use in many scenarios.  Some storage engines may use referencing, where
one file is stored as a diff against another file, or one file is composed
of references to others.  It can be difficult to determine whether a given
digest_hash is truly no longer used.

The safest way to clean up a CAS is to create a second CAS and migrate the
items you want to keep from the first to the second; then delete the
original CAS.  See the documentation on the storage engine you are using
to see if it supports an efficient way to do this.  For instance,
L<DataStore::CAS::Simple> can use hard-links on supporting filesystems,
resulting in a very efficient copy operation.

If no efficient mechanisms are available, then you might need to write a
mark/sweep algorithm and then make use of 'delete'.

Returns true if the item was actually deleted.

The optional 'flags' hashref can contain a wide variety of parameters, but
these are supported by all CAS subclasses:

=over

=item dry_run => $bool

Setting "dry_run" to true will run a simulation of the delete operation,
without actually deleting anything.

=item stats => \%stats_out

Setting "stats" to a hashref will instruct the CAS implementation to return
information about the operation within that supplied hashref.  Values in the
hashref are amended or added to, so you may use the same stats hashref for
multiple calls and then see the summary for all operations when you are done.

=over

=item delete_count

The number of official entries deleted.

=item delete_missing

The number of entries that didn't exist.

=back

=back

=cut

requires 'delete';

=head2 iterator

  $iter= $cas->iterator( \%optional_flags )
  while (defined ($digest_hash= $iter->())) { ... }

Iterate the contents of the CAS.  Returns a perl-style coderef iterator which
returns the next digest_hash string each time you call it.  Returns undef at
end of the list.

C<%flags> :

=over

=item prefix

Specify a prefix for all the returned digest hashes.  This acts as a filter.
You can use this to imitate Git's feature of identifying an object by a portion
of its hash instead of having to type the whole thing.  You will probably need
more digits though, because you're searching the whole CAS, and not just commit
entries.

=back

=cut

requires 'iterator';

=head2 open_file

  $handle= $cas->open_file( $fileObject, \%optional_flags )

Open the File object (returned by L</get>) and return a readable and seekable
filehandle to it.  The filehandle might be a perl filehandle, or might be a
tied object implementing the filehandle operations.

Flags:

=over

=item layer (TODO)

When implemented, this will allow you to specify a Parl I/O layer, like 'raw'
or 'utf8'.  This is equivalent to calling 'binmode' with that argument on the
filehandle.  Note that returned handles are 'raw' by default.

=back

=cut

requires 'open_file';

# File and Handle objects have DESTROY methods that call these methods of
# their associated CAS.  The CAS should implement these for cleanup of
# temporary files, or etc.
sub _file_destroy {}
sub _handle_destroy {}

package DataStore::CAS::File;
use strict;
use warnings;

our $VERSION= '0.03';

sub store { $_[0]{store} }
sub hash  { $_[0]{hash} }
sub size  { $_[0]{size} }

sub open {
	my $self= shift;
	return $self->{store}->open_file($self)
		if @_ == 0;
	return $self->{store}->open_file($self, { @_ })
		if @_ > 1;
	return $self->{store}->open_file($self, { layer => $_[0] })
		if @_ == 1 and !ref $_[0];
	Carp::croak "Wrong arguments to 'open'";
};

sub DESTROY {
	$_[0]{store}->_file_destroy(@_);
}

our $AUTOLOAD;
sub AUTOLOAD {
	my $attr= substr($AUTOLOAD, rindex($AUTOLOAD, ':')+1);
	return $_[0]{$attr} if exists $_[0]{$attr};
	unshift @_, $_[0]{store};
	goto (
		$_[0]->can("_file_$attr")
		or Carp::croak "Can't locate object method \"_file_$attr\" via package \"".ref($_[0]).'"'
	);
}

package DataStore::CAS::VirtualHandle;
use strict;
use warnings;

our $VERSION= '0.03';

sub new {
	my ($class, $cas, $fields)= @_;
	my $glob= bless Symbol::gensym(), $class;
	${*$glob}= $cas;
	%{*$glob}= %{$fields||{}};
	tie *$glob, $glob;
	$glob;
}
sub TIEHANDLE { return $_[0]; }

sub _cas  { ${*${$_[0]}} }  # the scalar view of the symbol points to the CAS object
sub _data { \%{*${$_[0]}} } # the hashref view of the symbol holds the fields of the handle

sub DESTROY { unshift @_, ${*{$_[0]}}; goto $_[0]->can('_handle_destroy') }

# By default, any method not defined will call to C<$cas->_handle_$method( $handle, @args );>
our $AUTOLOAD;
sub AUTOLOAD {
	unshift @_, ${*${$_[0]}}; # unshift @_, $self->_cas
	my $attr= substr($AUTOLOAD, rindex($AUTOLOAD, ':')+1);
	goto (
		$_[0]->can("_handle_$attr")
		or Carp::croak "Can't locate object method \"_handle_$attr\" via package \"".ref($_[0]).'"'
	);
}

#
# Tied filehandle API
#

sub READ     { (shift)->read(@_) }
sub READLINE { wantarray? (shift)->getlines : (shift)->getline }
sub GETC     { $_[0]->getc }
sub EOF      { $_[0]->eof }

sub WRITE    { (shift)->write(@_) }
sub PRINT    { (shift)->print(@_) }
sub PRINTF   { (shift)->printf(@_) }

sub SEEK     { (shift)->seek(@_) }
sub TELL     { (shift)->tell(@_) }

sub FILENO   { $_[0]->fileno }
sub CLOSE    { $_[0]->close }

#
# The following are some default implementations to make subclassing less cumbersome.
#

sub getlines {
	my $self= shift;
	wantarray or !defined wantarray or Carp::croak "getlines called in scalar context";
	my (@ret, $line);
	push @ret, $line
		while defined ($line= $self->getline);
	@ret;
}

# I'm not sure why anyone would ever want this function, but I'm adding
#  it for completeness.
sub getc {
	my $c;
	$_[0]->read($c, 1)? $c : undef;
}

# 'write' does not guarantee that all bytes get written in one shot.
# Needs to be called in a loop to accomplish "print" semantics.
sub _write_all {
	my ($self, $str)= @_;
	while (1) {
		my $wrote= $self->write($str);
		return 1 if defined $wrote and ($wrote eq length $str);
		return undef unless defined $wrote or $!{EINTR} or $!{EAGAIN};
		substr($str, 0, $wrote)= '';
	}
}

# easy to forget that 'print' API involves "$," and "$\"
sub print {
	my $self= shift;
	my $str= join( (defined $, ? $, : ""), @_ );
	$str .= $\ if defined $\;
	$self->_write_all($str);
}

# as if anyone would want to write their own printf implementation...
sub printf {
	my $self= shift;
	my $str= sprintf($_[0], $_[1..$#_]);
	$self->_write_all($str);
}

# virtual handles are unlikely to have one, and if they did, they wouldn't
# be using this class
sub fileno { undef; }

package DataStore::CAS::FileCreatorHandle;
use strict;
use warnings;
use parent -norequire => 'DataStore::CAS::VirtualHandle';

our $VERSION= '0.03';

# For write-handles, commit data to the CAS and return the digest hash for it.
sub commit   { $_[0]->_cas->commit_write_handle(@_) }

# These would happen anyway via the AUTOLOAD, but we enumerate them so that
# they officially appear as methods of this class.
sub close    { $_[0]->_cas->_handle_close(@_) }
sub seek     { $_[0]->_cas->_handle_seek(@_) }
sub tell     { $_[0]->_cas->_handle_tell(@_) }
sub write    { $_[0]->_cas->_handle_write(@_) }

# This is a write-only handle
sub eof      { return 1; }
sub read     { return 0; }
sub readline { return undef; }

1;
