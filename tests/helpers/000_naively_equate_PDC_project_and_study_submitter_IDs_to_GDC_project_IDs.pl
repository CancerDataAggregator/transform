#!/usr/bin/perl

use strict;

$| = 1;

# PARAMETERS

my $pdcFile = shift;

my $gdcFile = shift;

my $outFile = shift;

die("Usage: $0 <pdc_file> <gdc_file> <output_file>\n") if ( $outFile eq '' );

# EXECUTION

my $idToProject = {};

my $gdcProjectInProgram = {};

open IN, "<$gdcFile" or die("Can't open $gdcFile for reading.\n");

my $header = <IN>;

while ( chomp( my $line = <IN> ) ) {
    
    # entity_type	entity_submitter_id	GDC_project_id	GDC_program_name

    my ( $type, $id, $project, $program ) = split(/\t/, $line);

    $idToProject->{$type}->{$id}->{$project} = 1;

    $gdcProjectInProgram->{$project} = $program;
}

close IN;

open IN, "<$pdcFile" or die("Can't open $pdcFile for reading.\n");

$header = <IN>;

my $gdcProjectToPdcStudy = {};

my $pdcStudyID = {};

my $pdcStudyInProgram = {};

while ( chomp( my $line = <IN> ) ) {

    # program_name	project_submitter_id	study_submitter_id	pdc_study_id	entity_submitter_id	entity_id	entity_type

    my ( $programName, $pdcProject, $pdcStudy, $pdcID, $id, $uuid, $type ) = split(/\t/, $line);

    if ( exists( $idToProject->{$type}->{$id} ) ) {
        
        my $count = 0;

        $pdcStudyInProgram->{$pdcStudy} = $programName;

        foreach my $gdcProject ( keys %{$idToProject->{$type}->{$id}} ) {
            
            if ( not exists( $gdcProjectToPdcStudy->{$gdcProject}->{$pdcProject}->{$pdcStudy} ) ) {
                
                $gdcProjectToPdcStudy->{$gdcProject}->{$pdcProject}->{$pdcStudy} = 1;

                $count++;

            } else {
                
                $gdcProjectToPdcStudy->{$gdcProject}->{$pdcProject}->{$pdcStudy} += 1;

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

print OUT "match_count\tGDC_program_name\tGDC_project_id\tPDC_program_name\tPDC_project_submitter_id\tPDC_study_submitter_id\tPDC_pdc_study_id\n";

foreach my $gdcProject ( sort { $a cmp $b } keys %$gdcProjectToPdcStudy ) {
    
    foreach my $pdcProject ( sort { $a cmp $b } keys %{$gdcProjectToPdcStudy->{$gdcProject}} ) {
        
        foreach my $pdcStudy ( sort { $a cmp $b } keys %{$gdcProjectToPdcStudy->{$gdcProject}->{$pdcProject}} ) {
            
            print OUT "$gdcProjectToPdcStudy->{$gdcProject}->{$pdcProject}->{$pdcStudy}\t$gdcProjectInProgram->{$gdcProject}\t$gdcProject\t$pdcStudyInProgram->{$pdcStudy}\t$pdcProject\t$pdcStudy\t$pdcStudyID->{$pdcStudy}\n";
        }
    }
}

close OUT;


