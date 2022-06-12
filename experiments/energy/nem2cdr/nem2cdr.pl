#!/usr/bin/env perl
###################
# Really basic script to parse a NEM12 into a CDR payload
# Read the perldoc below for requirements
###################
use strict;
use warnings FATAL => 'all';
use FindBin 1.51 qw( $RealBin );
use lib $RealBin;
use Pod::Usage;
use Getopt::Long;
use Data::Dumper;
use N2C;
use N2CPayloads;
use JSON;


###
# Grok command line
###
my $conf = {};
GetOptions($conf, 'h|?', 'f=s', 'd:s', 's') or pod2usage(2);
pod2usage(2) if exists $conf->{'h'};
if(!exists $conf->{'f'}) {
    print STDERR "ERROR: Filename is required\n\n";
    pod2usage(2);
    exit 1;
}

my $n2c = N2C->new;
my $n2cPayload = N2CPayloads->new;
my $nemHash = $n2c->parseNem($conf->{'f'});

my $jsonPayload = JSON->new->utf8->encode($n2c->makeBaseCdrPayload($nemHash));

#print $n2cPayload->stripValueObject($jsonPayload);

if(exists $conf->{'s'}) {
    $n2c->jsonPayloadStats($jsonPayload,
        {
            '01 No Read Object'                     => $n2cPayload->stripValueObject($jsonPayload),
            '02 No Read Object or UOM'              => $n2cPayload->stripUom($n2cPayload->stripValueObject($jsonPayload)),
            '03 No Actual Reads'                    => $n2cPayload->noActualReads($jsonPayload),
            '04 No zero value reads'                => $n2cPayload->stripZeroes($jsonPayload),
            '05 No zero value reads or read object' => $n2cPayload->stripValueObject($n2cPayload->stripZeroes($jsonPayload)),
            '06 Nested Register IDs'                => JSON->new->utf8->encode($n2cPayload->makeRegisterSummarisedPayload($nemHash)),
            '07 Nested Register IDs with No Read Object' => $n2cPayload->stripValueObjectNestedRegister(JSON->new->utf8->encode($n2cPayload->makeRegisterSummarisedPayload($nemHash)))
        },
        $conf->{'d'}
    );
}




__END__

=head1 NAME

nem2cdr - A transformer for NEM files to CDR format and a few experiments

=head1 SYNOPSIS

nem2cdr [options] -d -f <filename>

 Options:
   -f               (Required) NEM12 Filename
   -d               Dump the Generated JSON Payload
   -s               Print stats about the JSON payload
   -h               brief help message

=head1 OPTIONS

=over 8

=item B<-f filename>

The NEM12 formatted file to process as input

=item B<-d>

Dump generated JSON payload

=item B<-s>

Produce some statistics about JSON payloads generated

=item B<-h>

Print a brief help message and exits.

=back

=head1 DESCRIPTION

B<nem2cdr> will read a given NEM12 file and transform it to CDR format.

nem2cdr B<requires Text:CSV, Gzip::Faster and Data::UUID>

=cut




