package DataStore::CAS;
use 5.008;
use Moo::Role;
use Carp;
use Try::Tiny;
require Scalar::Util;
require Symbol;

our $VERSION= '1.0000';

=head1 NAME

DataStore::CAS - Abstract base class for Content Addressable Storage

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

=head1 PURPOSE

In short, a CAS is a key/value mapping where small-ish keys are determined
from large-ish data but no two pieces of data will ever end up with the same
key, thanks to astronomical probabilities.  You can then use the small-ish
key as a reference to the large chunk of data, as a sort of compression
technique.

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

=head2 hash_of_null

The digest hash of the empty string.  The cached result of

  $cas->put('', { dry_run => 1 })

=cut

requires 'digest';

has hash_of_null => ( is => 'lazy' );

sub _build_hash_of_null {
	return $_[0]->put('', { dry_run => 1 });
}

=head1 METHODS

=head2 get( $digest_hash )

Returns a DataStore::CAS::File object for the given hash, if the hash
exists in storage. Else, returns undef.

This method is pure-virtual and must be implemented in the subclass.

=cut

requires 'get';

=head2 put( $thing, [ \%flags ])

Convenience method.
Inspects $thing and passes it off to a more specific method.  If you want
more control over which method is called, call it directly.

=over 2

=item *

Scalars are passed to 'put_scalar'.

=item *

Instances of DataStore::CAS::File or Path::Class::File are passed to 'put_file'.

=item *

Globrefs or instances of IO::Handle are passed to 'put_handle'

=item *

Dies if it encounters anything else.

=back

The return value is the digest hash of the stored data.

See '->new_write_handle' for the discussion of 'flags'.

Example:

  my $stats= {};
  $cas->put("abcdef", { stats => $stats });
  $cas->put(IO::File->new('~/file','r'), { stats => $stats });
  $cas->put(\*STDIN, { stats => $stats });
  $cas->put(Path::Class::file('~/file'), { stats => $stats });
  use Data::Printer;
  p $stats;

=cut

sub put {
	goto $_[0]->can('put_scalar') unless ref $_[1];
	goto $_[0]->can('put_file')   if ref($_[1])->isa('DataStore::CAS::File') or ref($_[1])->isa('Path::Class::File');
	goto $_[0]->can('put_handle') if ref($_[1])->isa('IO::Handle') or (Scalar::Util::reftype($_[1]) eq 'GLOB');
	croak("Can't 'put' object of type ".ref($_[1]));
}

=head2 put_scalar( $scalar [, \%flags ])

Puts the literal string "$scalar" into the CAS.
If scalar is a unicode string, it is first converted to an array of UTF-8
bytes. Beware that when you next call 'get', reading from the filehandle
will give you bytes and not the original Unicode scalar.

Returns the digest hash of the array of bytes.

See '->new_write_handle' for the discussion of 'flags'.

=cut

sub put_scalar {
	my ($self, $scalar, $flags)= @_;

	# Force to plain string
	$scalar= "$scalar" if ref $scalar;

	# Convert to octets.  Actually, opening a stream to a unicode scalar gives the
	#  same result, but best to be explicit about what we want and not rely on
	#  undocumented behavior.
	utf8::encode($scalar) if utf8::is_utf8($scalar);
	
	my $handle= $self->new_write_handle($flags);
	$handle->_write_all($scalar);
	return $self->commit_write_handle($handle);
}

=head2 put_file( $filename | Path::Class::File | DataStore::CAS::File [, \%flags ])

Insert a file from the filesystem, or from another CAS instance.
Default implementation simply opens the named file, and passes it to
put_handle.

Returns the digest hash of the data stored.

See '->new_write_handle' for the discussion of 'flags'.

Additional flags:

=over

=item hardlink => $bool

If hardlink is true, and the CAS is backed by plain files, it will hardlink
the file directly into the CAS.

This reduces the integrity of your CAS; use with care.  You can use the
C<->validate> method later to check for corruption.

=item known_hashes => \%algorithm_digests

If you already know the hash of your file, and don't want to re-calculate it,
pass a hashref like C< { $algorithm_name => $digest_hash } > for this flag,
and if this CAS is using one of those algorithms, it will use the hash you
specified instead of re-calculating it.

This reduces the integrity of your CAS; use with care.

=item reuse_hash

This is a shortcut for known_hashes if you specify an instance of
DataStore::CAS::File.  It builds a known_hashes of one item using the source
CAS's digest algorithm.

=back

Note: A good use of these flags is to transfer files from one instance of
DataStore::CAS::Simple to another.

  my $file= $cas1->get($hash);
  $cas2->put($file, { hardlink => 1, reuse_hash => 1 });

=cut

sub put_file {
	my ($self, $fname, $flags)= @_;
	my $fh;
	if (ref($fname) && ref($fname)->isa('DataStore::CAS::File')) {
		if ($flags->{reuse_hash}) {
			$flags->{known_hashes} ||= {};
			$flags->{known_hashes}{ $fname->store->digest }= $fname->hash;
		}
		$fh= $fname->open
			or croak "Can't open '$fname': $!";
	}
	elsif (ref($fname) && $fname->can('open')) {
		$fh= $fname->open('r')
			or croak "Can't open '$fname': $!";
	}
	else {
		open($fh, '<', "$fname")
			or croak "Can't open '$fname': $!";
	}
	$self->put_handle($fh, $flags);
}

=head2 put_handle( \*HANDLE | IO::Handle, [ \%flags ])

Pure virtual method.  Must be implemented by all subclasses.

Reads from $io_handle and stores into the CAS.  Calculates the digest hash
of the data as it goes.  Dies on any I/O errors.

Returns the calculated hash when complete.

If the string already exists in the CAS, most back-ends will be smart enough
to not store anything, and just return the hash.

See '->put' for the discussion of 'flags'.

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

=head2 new_write_handle( %flags )

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

=head2 commit_write_handle( $handle )

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

=head2 validate( $digest_hash [, %flags ])

Validate an entry of the CAS.  This is used to detect whether the storage
has become corrupt.  Returns 1 if the hash checks out ok, and returns 0 if
it fails, and returns undef if the hash doesn't exist.

Like the 'put' method, you can pass a hashref in $flags{stats} which
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

=head2 delete( $digest_hash [, %flags ])

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
DataStore::CAS::Simple can use hard-links on supporting filesystems,
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

=head2 iterator([ \%flags ])

Iterate the contents of the CAS.  Returns a perl-style coderef iterator which
returns the next digest_hash string each time you call it.  Returns undef at
end of the list.

Flags:

The only flag defined so far is 'prefix'.  You can use this to imitate Git's
feature of identifying an object by a portion of its hash instead of having
to type the whole thing.  You will probably need more digits though, because
you're searching the whole CAS, and not just commit entries.

=cut

requires 'iterator';

=head2 open_file( $file [, \%flags ])

Open the File object (returned by 'get') and return a readable and seekable
filehandle to it.  The filehandle might be a perl filehandle, or might be a
tied object implementing the filehandle operations.

Flags:

=over 10

=item layer (NOT IMPLEMENTED YET!)

Specify a perl I/O layer, like 'raw' or 'utf8'.  This is equivalent to calling
'binmode' with that argument on the filehandle.  Note that returned handles
are 'raw' by default.

=back

=cut

requires 'open_file';

# File and Handle objects have DESTROY methods that call these methods of
# their associated CAS.  The CAS should implement these for cleanup of
# temporary files, or etc.
sub _file_destroy {}
sub _handle_destroy {}

=head1 FILE OBJECTS

The 'get' method returns objects of type DataStore::CAS::File. (or a subclass)

These are bare minimal wrappers that essentially just curry a few parameters
to later calls to 'open' (or possibly 'put').

The file objects returned by a store implementation may vary, but they will
always have the following API available:

=over 8

=item store

Read-only attribute; Reference to the store which created this file.

=item hash

Read-only attribute; The digest hash of the bytes of this file.

=item size

Read-only attribute; The length of the file, in bytes.

=item open([ $layer_name | %flags | \%flags ])

A convenience method to call '$file->store->open_file($file, \%flags)'

=back

Other attributes or methods may exist for the storage engine you are using;
see the documentation for your particular store.

=cut

package DataStore::CAS::File;
use strict;
use warnings;

our $VERSION= '0.0100';

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

our $VERSION= '0.0100';

=head1 HANDLE OBJECTS

The handles returned by open_file and new_write_handle are compatible with
both the old GLOBREF style functions and the new IO::Handle API.  In other
words, you can use either

  $handle->read($buffer, 100)
  or
  read($handle, $buffer, 100)

So they are nicely compatible with other libraries you might use.  It is
unlikely that they are actually real handles though, so you probably can't
sysread/syswrite on them.  You can find out by checking "fileno($handle)".
One notable exception is DataStore::CAS::Simple->open_file, which always
returns a direct filehandle to the underlying file.

=cut

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
sub READLINE { wantarray? (shift)->readlines : (shift)->readline }
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

sub readlines {
	my $self= shift;
	wantarray or !defined wantarray or Carp::croak "readlines called in scalar context";
	my (@ret, $line);
	push @ret, $line
		while defined ($line= $self->readline);
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

# For write-handles, commit data to the CAS and return the File object for it.
sub commit   { $_[0]->_cas->_handle_commit(@_) }

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
