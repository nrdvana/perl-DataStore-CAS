require DataStore::CAS;
1;
# PODNAME: DataStore::CAS::File
# ABSTRACT: Object returned by DataStore::CAS describing a CAS entry
=head1 DESCRIPTION

These are bare minimal wrappers that essentially just curry a few parameters
to later calls to 'open' (or possibly 'put').

All file objects will have the attributes described here, but other
attributes or methods may exist for the storage engine you are using;
see the documentation for your particular store.

=head1 ATTRIBUTES

=head2 cas

Read-only attribute; Reference to the L<DataStore::CAS> which created this file.

=head2 hash

Read-only attribute; The digest hash of the bytes of this file.

=head2 size

Read-only attribute; The length of the file, in bytes.

=head1 METHODS

=head2 open

  $handle= $file->open( %flags | \%flags )

A convenience method to call C<$file-E<gt>cas-E<gt>open_file($file, \%flags)>.

=head1 IMPLEMENTATION

File objects are equipped with an AUTOLOAD which passes all unknown function
calls to C<$file-E<gt>cas-E<gt>_file_$METHODNAME($file, @_)>.

This allows stores to use the built-in File objects without a lot of delegation.

File objects also come with a DESTROY which calls C<$file-E<gt>cas-E<gt>_file_destroy($file)>

=cut

