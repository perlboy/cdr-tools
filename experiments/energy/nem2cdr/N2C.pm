package N2C;

use strict;
use warnings FATAL => 'all';
use Getopt::Long;
use Pod::Usage;
use Text::CSV qw(csv);
use Data::UUID;
use Gzip::Faster;

sub new {
    my ($class) = @_;
    my $self = {};
    bless $self, $class;
    return $self;
}

sub calcPercentage {
    my $self = shift;
    my $old = shift;
    my $new = shift;

    return (((length($new) - length($old)) / length($old)) * 100)
}

sub jsonPayloadStats {
    my $self = shift;
    my $baselinePayload = shift;
    my $payloads = shift;
    my $baselinePayloadGzip = gzip($baselinePayload);
    my $fileDir = shift;

    print "----------------------------------------\n";
    printf "Baseline Total Payload Size: %d\n", length($baselinePayload);
    printf "Baseline Compressed Payload Size: %d (%.2f%%)\n", length($baselinePayloadGzip), $self->calcPercentage($baselinePayload, $baselinePayloadGzip);
    print "----------------------------------------\n";

    if(length($fileDir) > 0) {
        open(FILE, ">$fileDir/00 Baseline.json");
        print FILE $baselinePayload;
        close(FILE);
    }

    foreach my $name (sort keys %{$payloads}) {
            my $payload = $payloads->{$name};
            my $gzipJson = gzip($payloads->{$name});
            printf "$name Compressed Size: %d (%.2f%%)\n", length($gzipJson), $self->calcPercentage($baselinePayloadGzip, $gzipJson);

            if(length($fileDir) > 0) {
                open(FILE, ">$fileDir/$name.json");
                print FILE $payload;
                close(FILE);
            }
    }
}

sub makeBaseCdrPayload {
    my $self = shift;
    my $nemHash = shift;

    my $ug = Data::UUID->new;

    my @jsonRows;

    foreach my $nmi (keys %{$nemHash}) {
        my $servicePoint = $ug->create_str();
        foreach my $registerId (keys %{$nemHash->{$nmi}}) {
            foreach my $day (sort keys %{$nemHash->{$nmi}->{$registerId}->{'reads'}}) {
                my %jsonStruct;
                $jsonStruct{'servicePointId'} = $servicePoint;
                $jsonStruct{'registerId'} = $registerId;
                $jsonStruct{'registerSuffix'} = $nemHash->{$nmi}->{$registerId}->{'suffix'};
                $jsonStruct{'meterID'} = $nemHash->{$nmi}->{$registerId}->{'meterId'};
                $jsonStruct{'readStartDate'} = $day;
                $jsonStruct{'unitOfMeasure'} = $nemHash->{$nmi}->{$registerId}->{'uom'};
                $jsonStruct{'readUType'} = 'intervalRead';
                $jsonStruct{'intervalRead'}{'readIntervalLength'} = $nemHash->{$nmi}->{$registerId}->{'interval'};
                my @reads = @{$nemHash->{$nmi}->{$registerId}->{'reads'}->{$day}->{'reads'}};
                my @substitutes = $nemHash->{$nmi}->{$registerId}->{'reads'}->{$day}->{'substitutes'} ? @{$nemHash->{$nmi}->{$registerId}->{'reads'}->{$day}->{'substitutes'}} : ();
                my @hashRead;
                my $totalSum = 0;
                for(my $i = 1; $i <= @reads; $i++) {
                    my $read = $reads[$i-1];
                    $totalSum += $read;
                    my %record = (
                        'value' => sprintf("%.3g", $read) + 0
                    );
                    if(grep(/^$i$/, @substitutes)) {
                        $record{'quality'} = 'FINAL_SUBSTITUTE';
                    }

                    push(@hashRead, \%record);
                }
                $jsonStruct{'intervalRead'}{'intervalReads'} = \@hashRead;
                $jsonStruct{'intervalRead'}{'aggregateValue'} = $totalSum;
                push(@jsonRows, \%jsonStruct);
            }
        }
    }

    return \@jsonRows;

}

sub parseNem {
    my $self = shift;
    my $file = shift;

    my $nemHash = {};

    # Read/parse CSV
    my $csv = Text::CSV->new({ auto_diag => 1 });
    open my $fh, "<:encoding(utf8)", $file or die "Unable to open $file: $!";
    my ($nmi, $registerId, $intervalLength, $uom, $suffix, $serial, $date);
    my $reads = {};
    my @substitutes = ();

    while (my $row = $csv->getline($fh)) {

        # Header row process
        if ($row->[0] eq '100') {
            if ($row->[1] ne 'NEM12') {
                die "Not a NEM12 input file";
            }
        }
        elsif ($row->[0] eq '200') {
            # Write nem hash if NMI is already set
            if ($nmi) {
                $nemHash->{$nmi}->{$registerId}->{'reads'} = $reads;
                $nemHash->{$nmi}->{$registerId}->{'suffix'} = $suffix;
                $nemHash->{$nmi}->{$registerId}->{'meterId'} = $serial;
                $nemHash->{$nmi}->{$registerId}->{'uom'} = $uom;
                $nemHash->{$nmi}->{$registerId}->{'interval'} = $intervalLength;
            }
            $nmi = $row->[1];
            $registerId = $row->[3];
            $uom = $row->[7];
            $intervalLength = $row->[8];
            $suffix = $row->[4];
            $serial = $row->[6];
            $reads = {};
            @substitutes = ();
        }
        elsif ($row->[0] eq '300') {
            $date = $row->[1];
            my $recordCount = (1440 / $intervalLength) + 1;
            my @meterReads = @{$row}[2 .. $recordCount];
            $reads->{$row->[1]}{'reads'} = \@meterReads;
        } elsif ($row->[0] eq '400') {
            my @substitutes = $reads->{$date}->{'substitutes'} ? @{$reads->{$date}->{'substitutes'}} : ();
            if($row->[3] =~ /^F/) {
                for(my $i = $row->[1]; $i <= $row->[2]; $i++) {
                    push(@substitutes, $i);
                }
            }
            $reads->{$date}->{'substitutes'} = \@substitutes;
        }
    }
    close $fh;

    # Write nem hash if NMI is already set
    if ($nmi) {
        $nemHash->{$nmi}->{$registerId}->{'reads'} = $reads;
        $nemHash->{$nmi}->{$registerId}->{'suffix'} = $suffix;
        $nemHash->{$nmi}->{$registerId}->{'meterId'} = $serial;
        $nemHash->{$nmi}->{$registerId}->{'uom'} = $uom;
        $nemHash->{$nmi}->{$registerId}->{'interval'} = $intervalLength;
    }

    return $nemHash;
}

1;