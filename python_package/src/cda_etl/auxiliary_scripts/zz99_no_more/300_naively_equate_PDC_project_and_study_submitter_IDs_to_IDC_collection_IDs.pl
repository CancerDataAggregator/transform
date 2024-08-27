#!/usr/bin/perl

use strict;

$| = 1;

# PARAMETERS

my $pdcFile = shift;

my $idcFile = shift;

my $outFile = shift;

die("Usage: $0 <pdc_file> <idc_file> <output_file>\n") if ( $outFile eq '' );

# EXECUTION

my $idToCollection = {};

open IN, "<$idcFile" or die("Can't open $idcFile for reading.\n");

my $header = <IN>;

while ( chomp( my $line = <IN> ) ) {
    
    # collection_id	submitter_case_id	idc_case_id	entity_type

    my ( $collection, $id, $case_id, $type ) = split(/\t/, $line);

    $idToCollection->{$type}->{$id}->{$collection} = 1;
}

close IN;

open IN, "<$pdcFile" or die("Can't open $pdcFile for reading.\n");

$header = <IN>;

my $idcCollectionToPdcStudy = {};

my $pdcStudyID = {};

my $pdcStudyInProgram = {};

while ( chomp( my $line = <IN> ) ) {

    # program_name	project_submitter_id	study_submitter_id	pdc_study_id	entity_submitter_id	entity_id	entity_type

    my ( $programName, $pdcProject, $pdcStudy, $pdcID, $id, $uuid, $type ) = split(/\t/, $line);

    if ( exists( $idToCollection->{$type}->{$id} ) ) {
        
        my $count = 0;

        $pdcStudyInProgram->{$pdcStudy} = $programName;

        foreach my $idcCollection ( keys %{$idToCollection->{$type}->{$id}} ) {
            
            if ( not exists( $idcCollectionToPdcStudy->{$idcCollection}->{$pdcProject}->{$pdcStudy} ) ) {
                
                $idcCollectionToPdcStudy->{$idcCollection}->{$pdcProject}->{$pdcStudy} = 1;

                $count++;

            } else {
                
                $idcCollectionToPdcStudy->{$idcCollection}->{$pdcProject}->{$pdcStudy} += 1;

                $count++;
            }

            $pdcStudyID->{$pdcStudy} = $pdcID;

            if ( $count > 1 ) {
                
                print STDERR "WHAT!\n";
            }

            if ( $pdcStudy eq 'TCGA COAD Proteome S016-1' ) {
                
#                print STDERR "$line\n";
            }
        }
    }
}

close IN;

open OUT, ">$outFile" or die("Can't open $outFile for writing.\n");

print OUT "match_count\tIDC_collection_id\tPDC_program_name\tPDC_project_submitter_id\tPDC_study_submitter_id\tPDC_pdc_study_id\n";

foreach my $idcCollection ( sort { $a cmp $b } keys %$idcCollectionToPdcStudy ) {
    
    foreach my $pdcProject ( sort { $a cmp $b } keys %{$idcCollectionToPdcStudy->{$idcCollection}} ) {
        
        foreach my $pdcStudy ( sort { $a cmp $b } keys %{$idcCollectionToPdcStudy->{$idcCollection}->{$pdcProject}} ) {
            
            print OUT "$idcCollectionToPdcStudy->{$idcCollection}->{$pdcProject}->{$pdcStudy}\t$idcCollection\t$pdcStudyInProgram->{$pdcStudy}\t$pdcProject\t$pdcStudy\t$pdcStudyID->{$pdcStudy}\n";
        }
    }
}

close OUT;


