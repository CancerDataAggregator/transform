#!/usr/bin/perl

use strict;

$| = 1;

# PARAMETERS

my $inFile = 'PDC_entity_submitter_id_to_project_submitter_id.tsv';

my $outFile = 'records_with_nonunique_PDC_project_ID_and_submitter_ID_pairs.tsv';

# EXECUTION

my $multiplicity = {};

open IN, "<$inFile" or die("Can't open $inFile for reading.\n");

my $header = <IN>;

while ( chomp( my $line = <IN> ) ) {
    
    # project_submitter_id	study_submitter_id	entity_submitter_id	entity_id	entity_type

    my ( $projectID, $studyID, $submitterID, @theRest ) = split(/\t/, $line);

    $multiplicity->{$projectID}->{$submitterID} += 1;
}

close IN;

open IN, "<$inFile" or die("Can't open $inFile for reading.\n");

open OUT, ">$outFile" or die("Can't open $outFile for writing.\n");

my $header = <IN>;

print OUT "MULTIPLICITY\t$header";

while ( chomp( my $line = <IN> ) ) {
    
    # project_submitter_id	study_submitter_id	entity_submitter_id	entity_id	entity_type

    my ( $projectID, $studyID, $submitterID, @theRest ) = split(/\t/, $line);

    if ( $multiplicity->{$projectID}->{$submitterID} > 1 ) {
        
        print OUT "$multiplicity->{$projectID}->{$submitterID}\t$line\n";
    }
}

close OUT;

close IN;

