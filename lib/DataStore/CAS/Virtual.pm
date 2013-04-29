package DataStore::CAS::Virtual;
use 5.008;
use Moo;
use Carp;
use Try::Tiny;
use Digest;

our $VERSION= '0.0100';

has digest  => ( is => 'ro', default => sub { 'SHA-1' } );
has entries => ( is => 'rw', default => sub { {} } );

with 'DataStore::CAS';

sub get {
	my ($self, $hash)= @_;
	defined (my $data= $self->entries->{$hash})
		or return undef;
	return bless { store => $self, hash => $hash, size => length($data), data => $data }, 'DataStore::CAS::File';
}

sub put_scalar {
	my ($self, $data, $flags)= @_;

	my $hash= ($flags and defined $flags->{known_hash})? $flags->{known_hash}
		: Digest->new($self->digest)->add($data)->hexdigest;

	$self->entries->{$hash}= $data
		unless $flags and $flags->{dry_run};

	$hash;
}

sub new_write_handle {
	my ($self, $flags)= @_;
	my $data= {
		buffer  => '',
		flags   => $flags
	};
	return DataStore::CAS::FileCreatorHandle->new($self, $data);
}

sub _handle_write {
	my ($self, $handle, $buffer, $count, $offset)= @_;
	my $data= $handle->_data;
	utf8::encode($buffer) if utf8::is_utf8($buffer);
	$offset ||= 0;
	$count ||= length($buffer)-$offset;
	$data->{buffer} .= substr($buffer, $offset, $count);
	return $count;
}

sub _handle_seek {
	croak "Seek unsupported (for now)"
}

sub _handle_tell {
	my ($self, $handle)= @_;
	return length($handle->_data->{buffer});
}

sub commit_write_handle {
	my ($self, $handle)= @_;
	return $self->put_scalar($handle->_data->{buffer}, $handle->_data->{flags});
}

sub open_file {
	my ($self, $file, $flags)= @_;
	open(my $fh, '<', \$self->entries->{$file->hash})
		or die "open: $!";
	return $fh;
}

sub iterator {
	my $self= shift;
	my @entries= sort keys %{$self->entries};
	sub { shift @entries };
}

sub delete {
	my ($self, $hash, $flags)= @_;
	my $deleted= ($flags && $flags->{dry_run})?
		exists $self->entries->{$hash}
		: defined delete $self->entries->{$hash};
	$flags->{stats}{$deleted? 'delete_count' : 'delete_missing'}++
		if $flags && $flags->{stats};
	$deleted;
}

1;