#!/usr/bin/perl

use strict;

$| = 1;

# PARAMETERS

my $inFile = 'PDC_entity_submitter_id_to_program_name_and_project_submitter_id.tsv';

my $outFile = 'records_with_nonunique_PDC_study_ID_and_submitter_ID_pairs.tsv';

# EXECUTION

my $ids = {};

open IN, "<$inFile" or die("Can't open $inFile for reading.\n");

my $header = <IN>;

while ( chomp( my $line = <IN> ) ) {
    
    # program_name	project_submitter_id	study_submitter_id	pdc_study_id	entity_submitter_id	entity_id	entity_type

    my ( $programName, $projectID, $studyID, $pdcID, $submitterID, $entityID, $entityType ) = split(/\t/, $line);

    $ids->{$studyID}->{$submitterID}->{$entityID} += 1;
}

close IN;

open IN, "<$inFile" or die("Can't open $inFile for reading.\n");

open OUT, ">$outFile" or die("Can't open $outFile for writing.\n");

my $header = <IN>;

print OUT "MULTIPLICITY\t$header";

while ( chomp( my $line = <IN> ) ) {
    
    # program_name	project_submitter_id	study_submitter_id	pdc_study_id	entity_submitter_id	entity_id	entity_type

    my ( $programName, $projectID, $studyID, $pdcID, $submitterID, $entityID, $entityType ) = split(/\t/, $line);

    if ( scalar( keys %{$ids->{$studyID}->{$submitterID}} ) > 1 ) {
        
        print OUT scalar( keys %{$ids->{$studyID}->{$submitterID}} ). "\t$line\n";
    }
}

close OUT;

close IN;

