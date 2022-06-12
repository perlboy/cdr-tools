package N2CPayloads;

use strict;
use warnings FATAL => 'all';
use Getopt::Long;
use Pod::Usage;
use Text::CSV qw(csv);
use Data::UUID;
use Gzip::Faster;
use JSON;


sub new {
    my ($class) = @_;
    my $self = {};
    bless $self, $class;
    return $self;
}

sub arrayToRanges {
    my $self = shift;
    my $numbers = shift;

    if (@{$numbers} < 1) {
        return [];
    }

    my @ranges;
    for (@{$numbers}) {
        if (@ranges && $_ == $ranges[-1][1] + 1) {
            ++$ranges[-1][1];
        }
        else {
            push @ranges, [ $_, $_ ];
        }
    }

    my @array = join ',', map {$_->[0] == $_->[1] ? $_->[0] : "$_->[0]-$_->[1]"} @ranges;
    return \@array;

}

sub stripZeroes {
    my $self = shift;
    my $jsonPayload = shift;

    my $structure = JSON->new->utf8->decode($jsonPayload);
    my @newStructure;

    foreach my $thisEntry (@{$structure}) {
        my @newInterval;
        my $count = 0;
        my @excludedRecords;
        foreach my $read (@{$thisEntry->{'intervalRead'}{'intervalReads'}}) {
            $count++;
            if ($read->{'value'} > 0) {
                push(@newInterval, $read);
            }
            else {
                push(@excludedRecords, $count);
            }
        }
        $thisEntry->{'intervalRead'}->{'removedReads'} = $self->arrayToRanges(\@excludedRecords);
        $thisEntry->{'intervalRead'}->{'intervalReads'} = \@newInterval;

        push(@newStructure, $thisEntry);
    }
    return JSON->new->utf8->encode(\@newStructure);
}

sub noActualReads {
    my $self = shift;
    my $jsonPayload = shift;

    my $structure = JSON->new->utf8->decode($jsonPayload);
    my @newStructure;

    foreach my $thisEntry (@{$structure}) {
        delete $thisEntry->{'intervalRead'}{'intervalReads'};
        push(@newStructure, $thisEntry);
    }

    return JSON->new->utf8->encode(\@newStructure);
}

sub stripUom {
    my $self = shift;
    my $jsonPayload = shift;

    my $structure = JSON->new->utf8->decode($jsonPayload);
    my @newStructure;

    foreach my $thisEntry (@{$structure}) {
        delete $thisEntry->{'unitOfMeasure'};
        push(@newStructure, $thisEntry);
    }

    return JSON->new->utf8->encode(\@newStructure);
}

sub makeRegisterSummarisedPayload {
    my $self = shift;
    my $nemHash = shift;

    my $ug = Data::UUID->new;

    my @jsonRows;

    foreach my $nmi (keys %{$nemHash}) {
        my $servicePoint = $ug->create_str();
        my @registers = keys %{$nemHash->{$nmi}};
        foreach my $day (sort keys %{$nemHash->{$nmi}->{$registers[0]}->{'reads'}}) {
            my %jsonStruct;
            $jsonStruct{'servicePointId'} = $servicePoint;
            $jsonStruct{'meterID'} = $nemHash->{$nmi}->{$registers[0]}->{'meterId'};
            $jsonStruct{'readStartDate'} = $day;
            $jsonStruct{'unitOfMeasure'} = $nemHash->{$nmi}->{$registers[0]}->{'uom'};
            $jsonStruct{'readUType'} = 'intervalRead';
            $jsonStruct{'intervalRead'}{'readIntervalLength'} = $nemHash->{$nmi}->{$registers[0]}->{'interval'};
            my %registerReads;
            foreach my $registerId (keys %{$nemHash->{$nmi}}) {

                $registerReads{$registerId}{'suffix'} = $nemHash->{$nmi}->{$registerId}->{'suffix'};

                my @reads = @{$nemHash->{$nmi}->{$registerId}->{'reads'}->{$day}->{'reads'}};
                my @substitutes = $nemHash->{$nmi}->{$registerId}->{'reads'}->{$day}->{'substitutes'} ? @{$nemHash->{$nmi}->{$registerId}->{'reads'}->{$day}->{'substitutes'}} : ();
                my @hashRead;
                my $totalSum = 0;
                for (my $i = 1; $i <= @reads; $i++) {
                    my $read = $reads[$i - 1];
                    $totalSum += $read;
                    my %record = (
                        'value' => sprintf("%.3g", $read) + 0
                    );
                    if (grep(/^$i$/, @substitutes)) {
                        $record{'quality'} = 'FINAL_SUBSTITUTE';
                    }

                    push(@hashRead, \%record);
                }

                $registerReads{$registerId}{'reads'} = \@hashRead;
                $registerReads{$registerId}{'aggregateValue'} = $totalSum;

            }

            $jsonStruct{'intervalRead'}{'intervalReads'} = \%registerReads;
            push(@jsonRows, \%jsonStruct);
        }
    }

    return \@jsonRows;

}

sub stripValueObjectNestedRegister {
    my $self = shift;
    my $jsonPayload = shift;

    my $structure = JSON->new->utf8->decode($jsonPayload);
    my @newStructure;

    foreach my $thisEntry (@{$structure}) {
        my @newInterval;
        my @newSubstitutes;
        foreach my $register (keys %{$thisEntry->{'intervalRead'}{'intervalReads'}}) {
            my $myRecord = 0;
            foreach my $read (@{$thisEntry->{'intervalRead'}{'intervalReads'}{$register}{'reads'}}) {
                $myRecord++;
                if ($read->{'quality'} && $read->{'quality'} eq 'FINAL_SUBSTITUTE') {
                    push(@newSubstitutes, $myRecord);
                }
                push(@newInterval, $read->{'value'});
            }
            $thisEntry->{'intervalRead'}->{'intervalReads'}{$register}{'reads'} = \@newInterval;
            my $arrayRef = $self->arrayToRanges(\@newSubstitutes);
            if (@{$arrayRef} > 0) {
                $thisEntry->{'intervalRead'}->{'intervalReads'}{$register}{'final_substitutes'} = $arrayRef;
            }

        }
        push(@newStructure, $thisEntry);
    }

    return JSON->new->utf8->encode(\@newStructure);
}

sub stripValueObject {
    my $self = shift;
    my $jsonPayload = shift;

    my $structure = JSON->new->utf8->decode($jsonPayload);
    my @newStructure;

    foreach my $thisEntry (@{$structure}) {
        my @newInterval;
        my @newSubstitutes;
        my $myRecord = 0;

        foreach my $read (@{$thisEntry->{'intervalRead'}{'intervalReads'}}) {
            $myRecord++;
            if ($read->{'quality'} && $read->{'quality'} eq 'FINAL_SUBSTITUTE') {
                push(@newSubstitutes, $myRecord);
            }
            push(@newInterval, $read->{'value'});
        }
        $thisEntry->{'intervalRead'}->{'intervalReads'} = \@newInterval;

        my $arrayRef = $self->arrayToRanges(\@newSubstitutes);
        if (@{$arrayRef} > 0) {
            $thisEntry->{'intervalRead'}->{'final_substitutes'} = $arrayRef;
        }

        push(@newStructure, $thisEntry);
    }

    return JSON->new->utf8->encode(\@newStructure);
}

1;

