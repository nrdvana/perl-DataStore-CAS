requires 'parent';
requires 'Carp';
requires 'Digest', '>=1.16';
requires 'Digest::SHA';
requires 'File::Spec', '>=3.33';
requires 'File::Temp';
requires 'Moo', '>=2';
requires 'Moo::Role';
requires 'Scalar::Util';
requires 'Symbol';
requires 'Try::Tiny';
requires 'MRO::Compat' if $] < 5.010;
on 'test' => sub {
	requires 'Test::More', '>=1';
};
