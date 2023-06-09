#!/usr/bin/env perl

use strict;

$| = 1;

# ARGUMENTS

my $inFile = shift;

my $outFile = $inFile;

$outFile =~ s/repeated_records/varying_columns/;

# PARAMETERS

my @id_cols = ( 'file_location' );

# EXECUTION

open IN, "<$inFile" or die("Can't open $inFile for reading.\n");

chomp( my $header = <IN> );

my @colnames = split(/\t/, $header);

my $current_id = '';

my $values_per_column = {};

my $varying_columns = {};

my $last_offender = {};

while ( chomp( my $line = <IN> ) ) {
    
    my @fields = split(/\t/, $line);

    my $record = {};

    foreach my $i ( 0 .. $#colnames ) {
        
        $record->{$colnames[$i]} = $fields[$i];
    }

    my $row_id = '';

    my $first = 1;

    foreach my $id_col ( @id_cols ) {
        
        if ( $first ) {
            
            $row_id = $record->{$id_col};

            $first = 0;

        } else {
            
            $row_id .= '___' . $record->{$id_col};
        }
    }

    if ( $current_id eq '' ) {
        
        $current_id = $row_id;

    } elsif ( $row_id ne $current_id ) {
        
        foreach my $colname ( keys %$values_per_column ) {
            
            my $value_count = scalar( keys %{$values_per_column->{$colname}} );

            if ( $value_count > 1 ) {
                
                if ( not exists( $varying_columns->{$colname} ) ) {
                    
                    $varying_columns->{$colname} = 1;

                } else {
                    
                    $varying_columns->{$colname} += 1;
                }

                $last_offender->{$colname} = $current_id;
            }
        }

        $values_per_column = {};

        $current_id = $row_id;
    }

    foreach my $colname ( keys %$record ) {
        
        if ( not exists( $values_per_column->{$colname} ) ) {
            
            $values_per_column->{$colname} = {
                
                $record->{$colname} => 1
            };

        } else {
            
            $values_per_column->{$colname}->{$record->{$colname}} += 1;
        }
    }
}

close IN;

open OUT, ">$outFile" or die("Can't open $outFile for writing.\n");

foreach my $colname ( sort { $a cmp $b } keys %$varying_columns ) {
    
    print OUT "$varying_columns->{$colname}\t$colname\t$last_offender->{$colname}\n";
}

close OUT;


