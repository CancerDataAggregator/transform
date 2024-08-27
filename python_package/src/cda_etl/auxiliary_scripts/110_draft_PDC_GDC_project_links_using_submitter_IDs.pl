#!/usr/bin/perl

use strict;

$| = 1;

# PARAMETERS

my $gdcFile = 'auxiliary_metadata/__GDC_supplemental_metadata/GDC_entities_by_program_and_project.tsv';

my $pdcFile = 'auxiliary_metadata/__PDC_supplemental_metadata/PDC_entities_by_program_project_and_study.tsv';

my $outDir = 'auxiliary_metadata/__aggregation_logs/projects';

system( "mkdir -p $outDir" ) if not -d $outDir;

my $outFile = "$outDir/naive_GDC_PDC_project_id_map.tsv";

# EXECUTION

my $idToProject = {};

my $gdcProjectInProgram = {};

open IN, "<$gdcFile" or die("Can't open $gdcFile for reading.\n");

my $header = <IN>;

while ( chomp( my $line = <IN> ) ) {
    
    # program.program_id	program.name	project.project_id	project.name	entity_submitter_id	entity_id	entity_type

    my ( $program_id, $program_name, $project_id, $project_name, $submitter_id, $id, $type ) = split( /\t/, $line );

    $idToProject->{$type}->{$submitter_id}->{$project_id} = 1;

    $gdcProjectInProgram->{$project_id} = $program_name;
}

close IN;

open IN, "<$pdcFile" or die("Can't open $pdcFile for reading.\n");

$header = <IN>;

my $gdcProjectToPdcStudy = {};

my $pdcStudyID = {};

my $pdcStudyInProgram = {};

while ( chomp( my $line = <IN> ) ) {

    # program.program_id	program.program_submitter_id	program.name	project.project_id	project.project_submitter_id	project.name	study.study_id	study.study_submitter_id	study.pdc_study_id	entity_submitter_id	entity_id	entity_type

    my ( $program_id, $program_submitter_id, $program_name, $project_id, $project_submitter_id, $project_name, $study_id, $study_submitter_id, $pdc_study_id, $entity_submitter_id, $entity_id, $type ) = split(/\t/, $line);

    if ( exists( $idToProject->{$type}->{$entity_submitter_id} ) ) {
        
        my $count = 0;

        $pdcStudyInProgram->{$study_submitter_id} = $program_name;

        foreach my $gdcProject ( keys %{$idToProject->{$type}->{$entity_submitter_id}} ) {
            
            if ( not exists( $gdcProjectToPdcStudy->{$gdcProject}->{$project_submitter_id}->{$study_submitter_id} ) ) {
                
                $gdcProjectToPdcStudy->{$gdcProject}->{$project_submitter_id}->{$study_submitter_id} = 1;

                $count++;

            } else {
                
                $gdcProjectToPdcStudy->{$gdcProject}->{$project_submitter_id}->{$study_submitter_id} += 1;

                $count++;
            }

            $pdcStudyID->{$study_submitter_id} = $pdc_study_id;

            # if ( $count > 1 or $entity_submitter_id eq '2697' ) {
            #     
            #     print STDERR "Multiple GDC projects assigned to a single submitter_ID: ($gdcProject) ($entity_submitter_id)\n";
            # }
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


