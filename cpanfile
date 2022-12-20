requires 'parent';
requires 'Carp';
requires 'Digest', '>=1.16';
requires 'Digest::SHA';
requires 'File::Spec', '>=3.33';
requires 'File::Temp';
requires 'Moo';
requires 'Moo::Role';
requires 'Scalar::Util';
requires 'Symbol';
requires 'Try::Tiny';
requires 'MRO::Compat' if $] < 5.10;
on 'test' => sub {
	requires 'Test::More';
};
