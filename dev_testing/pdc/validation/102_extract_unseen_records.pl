#!/usr/bin/env perl

use strict;

$| = 1;

# ARGUMENTS

my $refFile = shift;

my $extractFile = shift;

my $colname_to_check = shift;

die("Usage: $0 <refFile> <extractFile> <colname_to_check>\n") if ( $colname_to_check eq '' );

my $outFile = $extractFile;

$outFile =~ s/^.*\/([^\/]+)$/$1/;

my $refFile_basename = $refFile;

$refFile_basename =~ s/^.*\/([^\/]+)$/$1/;

$outFile =~ s/\.([^\.]+)$/.records_not_in_${refFile_basename}.${colname_to_check}.$1/;

# EXECUTION

open IN, "<$refFile" or die("Can't open $refFile for reading.\n");

chomp( my $header = <IN> );

my @colnames = split(/\t/, $header);

my $seen = {};

while ( chomp( my $line = <IN> ) ) {
    
    my @fields = split(/\t/, $line);

    my $record = {};

    foreach my $i ( 0 .. $#colnames ) {
        
        $record->{$colnames[$i]} = $fields[$i];
    }

    $seen->{$record->{$colname_to_check}} = 1;
}

close IN;

open IN, "<$extractFile" or die("Can't open $extractFile for reading.\n");

chomp( $header = <IN> );

@colnames = split(/\t/, $header);

open OUT, ">$outFile" or die("Can't open $outFile for writing.\n");

print OUT "$header\n";

while ( chomp( my $line = <IN> ) ) {
    
    my @fields = split(/\t/, $line);

    my $record = {};

    foreach my $i ( 0 .. $#colnames ) {
        
        $record->{$colnames[$i]} = $fields[$i];
    }

    if ( not exists( $seen->{$record->{$colname_to_check}} ) ) {
        
        print OUT "$line\n";
    }
}

close OUT;

close IN;


