package DataStore::CAS::CatalystControllerSimpleCASAdapter;

our $VERSION= '0.07';
#ABSTRACT: Moo Role providing compatibility for Catalyst::Controller::SimpleCAS

=head1 SYNOPSIS

  package MyCAS;
  use Moo;
  extends 'DataStore::CAS::Simple'; # or whichever backend you want
  with 'DataStore::CAS::CatalystControllerSimpleCASAdapter';
  with 'Catalyst::Controller::SimpleCAS::Store';

  package MyCatalystApp;
  ...
  __PACKAGE__->config(
    ...
    'Controller::SimpleCAS' => {
      store_class => 'MyCAS',
      ...
    },
    ...
  );

=head1 DESCRIPTION

This Role allows you to use a DataStore::CAS object as the back-end for
L<Catalyst::Controller::SimpleCAS>.  It supplies all the methods required for
the L<Catalyst::Controller::SimpleCAS::Store> role, and supplies better
implementations for some of the methods that ::SimpleCAS::Store would normally
provide, so it needs added first from a separate 'with' statement.

=for Pod::Coverage content_exists content_size checksum_to_path fetch_content fetch_content_fh calculate_checksum file_checksum add_content add_content_file add_content_file_mv

=cut

use Carp;
use Moo::Role;

requires qw( get calculate_hash calculate_file_hash put_scalar put_file );

# SimpleCAS passes this argument, and our constructor checks for unknown
# arguments, so just store it in a weak attribute.
has simplecas => ( is => 'rw', weak_ref => 1 );

sub content_exists {
	my ($self, $checksum)= @_;
	return !!$self->get($checksum);
}

sub content_size {
	my ($self, $checksum)= @_;
	my $f= $self->get($checksum);
	return $f? $f->size : undef;
}

# This is part of the required API, but makes the assumption that the CAS files
# are in the local filesystem.  That is only true for DataStore::CAS::Simple.
sub checksum_to_path {
	my ($self, $checksum, $create)= @_;
	return $self->can('path_for_hash')? $self->path_for_hash($checksum, $create)
		: croak "SimpleCAS requested 'checksum_to_path' but this back-end doesn't support that";
}

sub fetch_content {
	my ($self, $checksum)= @_;
	my $fh= $self->get($checksum)->open;
	local $/= undef;
	return scalar <$fh>;
}

sub fetch_content_fh {
	my ($self, $checksum)= @_;
	return $self->get($checksum)->open;
}

sub calculate_checksum {
	my ($self, $data)= @_;
	return $self->calculate_hash($data);
}

sub file_checksum {
	my ($self, $file)= @_;
	return $self->calculate_file_hash($file);
}

sub add_content {
	my ($self, $data)= @_;
	return $self->put_scalar($data);
}

sub add_content_file {
	my ($self, $file)= @_;
	return $self->put_file($file);
}

sub add_content_file_mv {
	my ($self, $file)= @_;
	return $self->put_file("$file", { move => 1 });
}

1;
