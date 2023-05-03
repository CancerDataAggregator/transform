#!/usr/bin/env perl

use strict;

$| = 1;

# ARGUMENTS

my $inFile = shift;

my $outFile = $inFile;

$outFile =~ s/^.*\/([^\/]+)$/$1/;

$outFile =~ s/\.([^\.]+)$/.repeated_records.$1/;

# PARAMETERS

my @id_cols = ( 'file_location' );

# EXECUTION

open IN, "<$inFile" or die("Can't open $inFile for reading.\n");

open OUT, ">$outFile" or die("Can't open $outFile for writing.\n");

chomp( my $header = <IN> );

my @colnames = split(/\t/, $header);

print OUT "$header\n";

my $seen = {};

my $printed = {};

my $lastLine = {};

while ( chomp( my $line = <IN> ) ) {
    
    my @fields = split(/\t/, $line);

    my $record = {};

    foreach my $i ( 0 .. $#colnames ) {
        
        $record->{$colnames[$i]} = $fields[$i];
    }

    my $row_id = '';

    foreach my $id_col ( @id_cols ) {
        
        $row_id .= $record->{$id_col};
    }

    if ( exists( $seen->{$row_id} ) ) {
        
        if ( not exists( $printed->{$row_id} ) ) {
            
            print OUT $lastLine->{$row_id};

            $printed->{$row_id} = 1;
        }

        print OUT "$line\n";
    }

    $seen->{$row_id} = 1;

    $lastLine->{$row_id} = "$line\n";
}

close OUT;

close IN;


